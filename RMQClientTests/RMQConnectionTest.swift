import XCTest

class RMQConnectionTest: XCTestCase {

    func testImmediateConnectionErrorIsSentToDelegate() {
        let transport = ControlledInteractionTransport()
        transport.stubbedToThrowErrorOnConnect = "bad connection"
        let delegate = ConnectionDelegateSpy()
        let allocator = RMQMultipleChannelAllocator(channelSyncTimeout: 2)
        let conn = RMQConnection(
            transport: transport,
            user: "foo",
            password: "bar",
            vhost: "",
            channelMax: 123,
            frameMax: 321,
            heartbeat: 10,
            handshakeTimeout: 10,
            channelAllocator: allocator,
            frameHandler: allocator,
            delegate: delegate,
            commandQueue: FakeSerialQueue(),
            waiterFactory: RMQSemaphoreWaiterFactory(),
            heartbeatSender: HeartbeatSenderSpy()
        )
        XCTAssertNil(delegate.lastConnectionError)
        conn.start()
        XCTAssertEqual("bad connection", delegate.lastConnectionError!.localizedDescription)
    }

    func testErrorSentToDelegateOnHandshakeTimeout() {
        let transport = ControlledInteractionTransport()
        let allocator = RMQMultipleChannelAllocator(channelSyncTimeout: 10)
        let delegate = ConnectionDelegateSpy()
        let q = FakeSerialQueue()
        let conn = RMQConnection(
            transport: transport,
            user: "foo",
            password: "bar",
            vhost: "",
            channelMax: 123,
            frameMax: 321,
            heartbeat: 10,
            handshakeTimeout: 0,
            channelAllocator: allocator,
            frameHandler: allocator,
            delegate: delegate,
            commandQueue: q,
            waiterFactory: RMQSemaphoreWaiterFactory(),
            heartbeatSender: HeartbeatSenderSpy()
        )
        conn.start()
        try! q.step()

        XCTAssertEqual("Handshake timed out.", delegate.lastConnectionError?.localizedDescription)
    }

    func testTransportDelegateWriteErrorsAreTransformedIntoConnectionDelegateErrors() {
        let transport = ControlledInteractionTransport()
        let q = FakeSerialQueue()
        let delegate = ConnectionDelegateSpy()
        TestHelper.startedConnection(transport,
                                     commandQueue: q,
                                     delegate: delegate)
        transport.stubbedToProduceErrorOnWrite = "fail please"
        try! q.step()
        transport.handshake()

        XCTAssertEqual("fail please", delegate.lastWriteError!.localizedDescription)
    }

    func testTransportDelegateDisconnectErrorsAreTransformedIntoConnectionDelegateErrors() {
        let transport = ControlledInteractionTransport()
        let delegate = ConnectionDelegateSpy()
        let conn = TestHelper.startedConnection(transport, delegate: delegate)
        let e = NSError(domain: RMQErrorDomain, code: 0, userInfo: [NSLocalizedDescriptionKey: "foo"])

        conn.transport(nil, disconnectedWithError: e)

        XCTAssertEqual("foo", delegate.lastDisconnectError!.localizedDescription)
    }

    func testTransportDisconnectNotificationsNotTransformedWhenCloseRequested() {
        let delegate = ConnectionDelegateSpy()
        let conn = RMQConnection(delegate: delegate)
        conn.close()
        conn.transport(nil, disconnectedWithError: nil)

        XCTAssertFalse(delegate.disconnectCalled)
    }

    func testTransportDisconnectNotificationsTransformedWhenCloseNotRequested() {
        let transport = ControlledInteractionTransport()
        let delegate = ConnectionDelegateSpy()
        let conn = TestHelper.startedConnection(transport, delegate: delegate)
        conn.transport(transport, disconnectedWithError: nil)

        XCTAssertNil(delegate.lastDisconnectError)
        XCTAssertTrue(delegate.disconnectCalled)
    }

    func testSignalsActivityToHeartbeatSenderOnOutgoingFrameset() {
        let heartbeatSender = HeartbeatSenderSpy()
        let conn = RMQConnection(transport: ControlledInteractionTransport(), user: "", password: "", vhost: "", channelMax: 10, frameMax: 11, heartbeat: 12, handshakeTimeout: 10, channelAllocator: ChannelSpyAllocator(), frameHandler: FrameHandlerSpy(), delegate: ConnectionDelegateSpy(), commandQueue: FakeSerialQueue(), waiterFactory: FakeWaiterFactory(), heartbeatSender: heartbeatSender)

        XCTAssertFalse(heartbeatSender.signalActivityReceived)
        conn.sendFrameset(RMQFrameset(channelNumber: 1, method: MethodFixtures.channelOpen()))
        XCTAssert(heartbeatSender.signalActivityReceived)
    }

    func testSendsConfiguredVHostWithConnectionOpen() {
        let transport = ControlledInteractionTransport()
        let q = FakeSerialQueue()
        let conn = RMQConnection(transport: transport, user: "", password: "", vhost: "/myvhost", channelMax: 10, frameMax: 11, heartbeat: 12, handshakeTimeout: 10, channelAllocator: ChannelSpyAllocator(), frameHandler: FrameHandlerSpy(), delegate: ConnectionDelegateSpy(), commandQueue: q, waiterFactory: FakeWaiterFactory(), heartbeatSender: HeartbeatSenderSpy())
        conn.start()
        try! q.step()

        transport.handshake()

        let parser = RMQParser(data: transport.outboundData.last!)
        let outgoingConnectionOpen: RMQConnectionOpen = RMQFrame(parser: parser).payload as! RMQConnectionOpen

        XCTAssertEqual("/myvhost", outgoingConnectionOpen.virtualHost.stringValue)
    }

}
