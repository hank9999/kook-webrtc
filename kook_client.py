# This file is under MIT License, based on jimdragongod's work
# https://github.com/jimdragongod/mediasoup-client-pysdk/blob/main/smcdk/api/mediasoup_client.py


import asyncio
from logger import Logger
from smcdk import ConsumerRequestListener, DataConsumerRequestListener, BandwidthNotificationListener, \
    PeerNotificationListener, ProducerNotificationListener, ConsumerNotificationListener, \
    DataConsumerNotificationListener
from smcdk_multimedia_runtime import MultimediaRuntime
from smcdk.api.room_peer import Room, Peer, PeerAppData

from kook_signaler import ProtooSignaler, MessageType, Request
from kook_utils import gateway_url_parse

logger = Logger.get_logger(__name__)


class KookClient:
    def __init__(self):
        """
        multimedia runtime, room and peer part
        """
        self._multimediaRuntime = MultimediaRuntime()
        # room should be initialized to be injected into peer
        self._room = Room()
        # peer should be initialized to be injected into listeners
        self._mePeer = Peer(room=self._room)
        """
        signaler part
        """
        self._signaler = ProtooSignaler()
        """
        loop and tasks part
        """
        self._loop = None
        self._loopTasks = []
        """
        request listeners
        """
        self._consumerRequestListener = ConsumerRequestListener(self._mePeer)
        self._dataConsumerRequestListener = DataConsumerRequestListener(self._mePeer)
        """
        notification listeners
        """
        self._bandwidthNotificationListener = BandwidthNotificationListener(self._mePeer)
        self._peerNotificationListener = PeerNotificationListener(self._mePeer)
        self._producerNotificationListener = ProducerNotificationListener(self._mePeer)
        self._consumerNotificationListener = ConsumerNotificationListener(self._mePeer)
        self._dataConsumerNotificationListener = DataConsumerNotificationListener(self._mePeer)
        self._notificationListeners = [self._bandwidthNotificationListener, self._peerNotificationListener,
                                       self._producerNotificationListener, self._consumerNotificationListener,
                                       self._dataConsumerNotificationListener]
        self.is_ready = False

    def player_status(self) -> str:
        return self._multimediaRuntime.player_status()

    async def join(self, room_address_info: dict, peer_info: dict = None, producer_config: dict = None):
        """
        enter the specified room and auto-produce if needed

        :param room_address_info:
            {
                'server_url':
                    str, ws url,
                'ssl_verification':
                    bool, will be sent to KookWebRtcSignalerInterface.connectToRoom‘s 'enableSslVerification' argument,
                    to check if ssl verification is needed before establish connection to server,
                    the default value is True, that means ssl verification is needed
            }
        :param peer_info:
            {
                'display_name':
                    str, the peer id when current KookClient instance join the room, default is 'PySDK Client'
                'device':
                    str, the device name when current KookClient instance join the room,
                    default is 'pc'
            }
        :param producer_config:
            {
                'auto_produce': bool, default is True
                'media_file_path': str, the full path of media file, required
            }
        # :param consumerConfig:
        #     {
        #         'autoConsume': bool, default is True
        #         'recordDirectoryPath': str, the root path of to-record media files, required
        #     }
        :return: None
        """
        """
        stuff room and peer part
        """
        if peer_info is None:
            peer_info = {}
        if producer_config is None:
            producer_config = {}
        gateway_url = room_address_info['server_url']
        url_info = gateway_url_parse(gateway_url)

        # not first enter
        if self._room.roomId is not None:
            logger.warning('exit from room(id=%s) before join room(id=%s)', self._room.roomId, url_info.room_id)
            await self.exit_room(self._room.roomId)
        self._room.serverAddress = gateway_url
        self._room.roomId = url_info.room_id
        self._mePeer.peerId = url_info.peer_id
        self._mePeer.data = PeerAppData(
            displayName=peer_info.get('display_name', 'kook-webrtc'),
            device=peer_info.get('device', 'pc')
        )

        logger.info('peer(id=%s, displayName=%s) join room(id=%s)', url_info.peer_id,
                    peer_info.get('display_name', 'kook-webrtc'), url_info.room_id)
        """
        stuff multimedia runtime part
        """
        self._multimediaRuntime.initializeProducerAndConsumerOptions(
            autoProduce=producer_config.get('auto_produce', False),
            mediaFilePath=producer_config.get('media_file_path', True),
            autoConsume=False,
            recordDirectoryPath=None
        )
        """
        create connection to server by signaler
        """
        self._loop = asyncio.get_running_loop()
        logger.info('connectToRoom, serverAddress=%s, roomId=%s, peerId=%s',
                    self._room.serverAddress,
                    self._room.roomId,
                    self._mePeer.peerId
                    )
        await self._signaler.connectToRoom(self._loop, self._room.serverAddress,
                                           room_address_info.get('ssl_verification', True))
        """
        create loop tasks
        """
        bandwidth_notification_loop = self._loop.create_task(
            self._bandwidthNotificationListener.runLoop(asyncio.Queue()), name='BandwidthNotificationListener')
        peer_notification_loop = self._loop.create_task(
            self._peerNotificationListener.runLoop(asyncio.Queue()), name='PeerNotificationListener')
        producer_notification_loop = self._loop.create_task(
            self._producerNotificationListener.runLoop(asyncio.Queue()), name='ProducerNotificationListener')
        consumer_notification_loop = self._loop.create_task(
            self._consumerNotificationListener.runLoop(asyncio.Queue()), name='ConsumerNotificationListener')
        dataconsumer_notification_loop = self._loop.create_task(
            self._dataConsumerNotificationListener.runLoop(asyncio.Queue()), name='DataConsumerNotificationListener')
        sever_event_loop = self._loop.create_task(self._server_event_loop(), name='ServerEventListener')
        self._loopTasks = [sever_event_loop, bandwidth_notification_loop, peer_notification_loop,
                           producer_notification_loop, consumer_notification_loop, dataconsumer_notification_loop]

        """
        load device
        signaling: getRouterRtpCapabilities
        """
        await self._load_device_by_router_rtp_capabilities()
        if not (self._multimediaRuntime.canProduce or self._multimediaRuntime.canConsume):
            return
        """
        create sendTransport & recvTransport
        signaling: createWebRtcTransport, twice for both direction
        """
        if self._multimediaRuntime.canProduce:
            await self._create_send_transport()
        if self._multimediaRuntime.canConsume:
            await self._create_recv_transport()
        """
        formally join
        signaling: join
        """
        await self._join_formally()
        """
        produce(push media stream to the server) automatically if needed 
        """
        if self._multimediaRuntime.autoProduce:
            # self._loop.create_task(self._produce(kind='audio', source='mic'), name='produceTask_audio_mic')
            # self._loop.create_task(self._produce(kind='video', source='webCam'), name='produceTask_video_webCam')
            # self._loop.create_task(self._produce(kind='video', source='screenShare'),
            #                        name='produceTask_video_screenShare')
            self._loop.create_task(self._produce(), name='AutoProduceCoroutine')
        """
        waiting loop tasks
        """
        self.is_ready = True
        await asyncio.gather(sever_event_loop, bandwidth_notification_loop, peer_notification_loop,
                             producer_notification_loop, consumer_notification_loop, dataconsumer_notification_loop,
                             return_exceptions=True)

    def play(self):
        if self._loop is not None:
            return self._loop.create_task(self._produce(), name='PlayerCoroutine')
        else:
            logger.error(f'can\'t play, missing loop({self._loop})')


    def add_audio(self):
        self._create_send_transport()



    async def exit_room(self, room_id):
        """
        exit current room, prepare to join another
        :param room_id: current room's id to exit, required
        :return: None
        """

        def stop_task_func():
            for task in self._loopTasks:
                task.cancel()
            # quick GC, not needed currently
            # for notificationListeners in self._notificationListeners:
            #     notificationListeners.resetQueue(asyncio.Queue)

        await self._multimediaRuntime.close(stop_task_func)
        self._room.serverAddress = None
        self._room.roomId = None
        logger.info('exit room %s finished', room_id)

    async def close(self):
        """
        stop current MediasoupClient forever, for purpose of exiting process/thread only

        :return: None
        """
        if self._room.roomId is not None:
            await self.exit_room(room_id=self._room.roomId)
        else:
            logger.warning('already closed')

    async def _load_device_by_router_rtp_capabilities(self):
        request_id = await self._signaler.getRouterRtpCapabilities()
        logger.info('signal request: getRouterRtpCapabilities, requestId=%s', request_id)
        response = await self._signaler.getResponse(request_id)
        await self._multimediaRuntime.loadDevice(response.data)

    async def _create_send_transport(self):
        if self._multimediaRuntime.sendTransportId is not None:
            logger.warning('the sendTransport has already been created')
            return
        request_id = await self._signaler.createSendTransport(self._multimediaRuntime.sctpCapabilities.dict())
        logger.info('signal request: createWebRtcTransport, direction=send, requestId=%s', request_id)
        response = await self._signaler.getResponse(request_id)

        async def on_connect(dtls_parameters):
            request_id_to_connect_swt = await self._signaler.connectWebRtcTransport(
                self._multimediaRuntime.sendTransportId,
                dtls_parameters.dict(exclude_none=True)
            )

            logger.info('signal request: connectWebRtcTransport, direction: send, requestId: %s',
                        request_id_to_connect_swt)
            await self._signaler.getResponse(request_id_to_connect_swt)

        async def on_produce(kind: str, rtp_parameters, app_data: dict):
            request_id_to_produce = await self._signaler.produce(self._multimediaRuntime.sendTransportId, kind,
                                                                 rtp_parameters.dict(exclude_none=True), app_data)
            logger.info('signal request: produce, requestId=%s', request_id_to_produce)
            response_ = await self._signaler.getResponse(request_id_to_produce)
            return response_.data['id']

        async def on_produce_data(
                sctp_stream_parameters,
                label: str,
                protocol: str,
                app_data: dict
        ):
            request_id_to_produce_data = await self._signaler.produceData(
                self._multimediaRuntime.sendTransportId, label,
                protocol,
                sctp_stream_parameters.dict(exclude_none=True),
                app_data
            )
            logger.info('signal request: produceData, requestId=%s', request_id_to_produce_data)
            response_ = await self._signaler.getResponse(request_id_to_produce_data)
            return response_.data['id']

        self._multimediaRuntime.createSendTransport(transportId=response.data['id'],
                                                    iceParameters=response.data['iceParameters'],
                                                    iceCandidates=response.data['iceCandidates'],
                                                    dtlsParameters=response.data['dtlsParameters'],
                                                    sctpParameters=response.data['sctpParameters'],
                                                    onConnectFunc=on_connect,
                                                    onProduceFunc=on_produce,
                                                    onProduceDataFunc=on_produce_data)

    async def _create_recv_transport(self):
        if self._multimediaRuntime.recvTransportId is not None:
            logger.warning('the recvTransport has already been created')
            return
        request_id = await self._signaler.createRecvTransport(self._multimediaRuntime.sctpCapabilities.dict())
        logger.info('signal request: createWebRtcTransport, direction=recv, requestId=%s', request_id)
        response = await self._signaler.getResponse(request_id)

        async def on_connect(dtls_parameters):
            request_id_to_connect_rwt = await self._signaler.connectWebRtcTransport(
                self._multimediaRuntime.recvTransportId,
                dtls_parameters.dict(exclude_none=True)
            )
            logger.info('signal request: connectWebRtcTransport, direction: recv, requestId=%s',
                        request_id_to_connect_rwt)
            await self._signaler.getResponse(request_id_to_connect_rwt)

        self._multimediaRuntime.createRecvTransport(transportId=response.data['id'],
                                                    iceParameters=response.data['iceParameters'],
                                                    iceCandidates=response.data['iceCandidates'],
                                                    dtlsParameters=response.data['dtlsParameters'],
                                                    sctpParameters=response.data['sctpParameters'],
                                                    onConnectFunc=on_connect)

    async def _join_formally(self):
        request_id = await self._signaler.join(self._mePeer.data.displayName,
                                               self._mePeer.data.device,
                                               self._multimediaRuntime.rtpCapabilities.dict(exclude_none=True),
                                               self._multimediaRuntime.sctpCapabilities.dict(exclude_none=True))
        logger.info('signal request: join, requestId=%s', request_id)
        message = await self._signaler.getResponse(request_id)
        for peerInfo in message.data['peers']:
            self._room.addPeer(peerInfo['id'], data=PeerAppData(displayName=peerInfo.get('display_name', 'kook-webrtc'),
                                                                device=peerInfo.get('device', 'pc')))
        if self._room.getPeerByPeerId(self._mePeer.peerId) is None:
            self._room.addPeer(self._mePeer.peerId, self._mePeer)
        else:
            raise Exception(f'duplicate peerId of mePeer at room {self._room.roomId}')

    # enableShare
    async def _produce(self):
        if not self._multimediaRuntime.canProduce:
            logger.warning('can\'t play, please check mediaFilePath and rtpCapabilities')
            return
        if self._multimediaRuntime.sendTransportId is None:
            await self._create_send_transport()
        await self._multimediaRuntime.produce()

        # record

    async def _consume(self, message: Request):
        if not self._multimediaRuntime.canConsume or not self._multimediaRuntime.autoConsume:
            return
        if self._multimediaRuntime.recvTransportId is None:
            await self._create_recv_transport()
        consumer_id = message.data['id']
        peer_id = message.data['peerId']
        producer_peer = self._mePeer.room.getPeerByPeerId(peer_id)
        producer_id = message.data['producerId']
        kind = message.data['kind']
        rtp_parameters = message.data['rtpParameters']
        await self._multimediaRuntime.consume(self._mePeer, consumer_id, producer_peer,
                                              producer_id, kind, rtp_parameters)
        other_peer = self._room.getPeerByPeerId(peer_id)
        self._room.bindConsumerIdToPeer(consumer_id, other_peer)
        request_id = message.requestId
        await self._consumerRequestListener.onNewConsumer(self._signaler.responseToNewConsumer(request_id), message,
                                                          other_peer)

    async def _consume_data(self, message: Request):
        if self._multimediaRuntime.recvTransportId is None:
            await self._create_recv_transport()
        data_consumer_id = message.data['id']
        data_producer_id = message.data['dataProducerId']
        sctp_stream_parameters = message.data['sctpStreamParameters']
        label = message.data['label']
        protocol = message.data['protocol']
        app_data = message.data['appData']

        def on_message(recv_message: str):
            logger.info('DataChannel {%s}-{%s}: {%s}', label, protocol, recv_message)
            self._dataConsumerNotificationListener.onMessage(other_peer, recv_message, label, protocol, app_data)

        await self._multimediaRuntime.consumeData(data_consumer_id, data_producer_id, sctp_stream_parameters, label,
                                                  protocol, app_data, on_message)
        other_peer = self._room.getPeerByPeerId(message.data['peerId'])
        self._room.bindDataConsumerIdToPeer(data_consumer_id, other_peer)
        request_id = message.requestId
        await self._dataConsumerRequestListener.onNewDataConsumer(self._signaler.responseToNewDataConsumer(request_id),
                                                                  message,
                                                                  other_peer)

    async def _server_event_loop(self):
        while True:
            message = await self._signaler.receiveMessage()
            if message.get('response'):
                logger.info('receive response for requestId: %d', message['id'])
                logger.debug('response details: ok=%s, data=%s', message['data'])
                self._signaler.setResponse(message)
            elif message.get('request'):
                logger.info('receive request, requestId=%d, method=%s', message['id'], message['method'])
                if message['method'] == MessageType.SERVER_REQURST_newConsumer.value:
                    # don't use: asyncio.create_task(self._consume(...))
                    # to prevent that consumer's notification precede it‘s request
                    await self._consume(Request(message['id'], message['method'], message['data']))
                elif message['method'] == MessageType.SERVER_REQURST_newDataConsumer.value:
                    await self._consume_data(Request(message['id'], message['method'], message['data']))
                else:
                    logger.error('unhandled request: %s' + message)
            elif message.get('notification'):
                logger.info('receive notification, method=%s', message['method'])
                if message['method'] in {MessageType.SERVER_NOTIFICATION_downlinkBwe.value}:
                    await self._bandwidthNotificationListener.enqueue(message)
                elif message['method'] in {MessageType.SERVER_NOTIFICATION_activeSpeaker.value,
                                           MessageType.SERVER_NOTIFICATION_newPeer.value,
                                           MessageType.SERVER_NOTIFICATION_peerDisplayNameChanged.value,
                                           MessageType.SERVER_NOTIFICATION_peerClosed.value}:
                    await self._peerNotificationListener.enqueue(message)
                elif message['method'] in {MessageType.SERVER_NOTIFICATION_producerScore.value}:
                    await self._producerNotificationListener.enqueue(message)
                elif message['method'] in {MessageType.SERVER_NOTIFICATION_consumerScore.value,
                                           MessageType.SERVER_NOTIFICATION_consumerLayersChanged.value,
                                           MessageType.SERVER_NOTIFICATION_consumerPaused.value,
                                           MessageType.SERVER_NOTIFICATION_consumerResumed.value,
                                           MessageType.SERVER_NOTIFICATION_consumerClosed.value}:
                    await self._consumerNotificationListener.enqueue(message)
                elif message['method'] in {MessageType.SERVER_NOTIFICATION_dataConsumerClosed.value}:
                    await self._dataConsumerNotificationListener.enqueue(message)
                else:
                    logger.error('unhandled notification: %s' + message)
            # bypass other no-exists message type
