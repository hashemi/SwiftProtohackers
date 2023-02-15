//
//  11-PestControl.swift
//  
//
//  Created by Ahmad Alhashemi on 10/02/2023.
//

import NIOCore
import NIOPosix

extension String: Error {
    var errorDescription: String? { return self }
}

enum PestControl {
    enum Message: Equatable {
        enum Types: UInt8 {
            case hello = 0x50
            case error = 0x51
            case ok = 0x52
            case dialAuthority = 0x53
            case targetPopulations = 0x54
            case createPolicy = 0x55
            case deletePolicy = 0x56
            case policyResult = 0x57
            case siteVisit = 0x58
        }
        
        enum Action: UInt8, Equatable {
            case cull = 0x90
            case conserve = 0xa0
        }
        
        struct TargetPopulation: Equatable {
            let species: String
            let min: UInt32
            let max: UInt32
        }
        
        struct ObservedPopulation: Equatable {
            let species: String
            let count: UInt32
        }
        
        static let standardHello: Message = .hello(protocol: "pestcontrol", version: 1)
        
        case hello(protocol: String, version: UInt32)
        case error(msg: String)
        case ok
        case dialAuthority(site: UInt32)
        case targetPopulations(
            site: UInt32,
            populations: [TargetPopulation]
        )
        case createPolicy(species: String, action: Action)
        case deletePolicy(policy: UInt32)
        case policyResult(policy: UInt32)
        case siteVisit(site: UInt32, populations: [ObservedPopulation])
        
        init?(buffer: inout ByteBuffer) {
            func readString() -> String? {
                guard let length = buffer.readInteger(as: UInt32.self) else { return nil }
                return buffer.readString(length: Int(length))
            }
            
            func readUInt32() -> UInt32? {
                buffer.readInteger(as: UInt32.self)
            }
            
            func readUInt8() -> UInt8? {
                buffer.readInteger(as: UInt8.self)
            }
            
            // test checksum
            guard buffer.readableBytesView.reduce(0, { $0 &+ $1 }) == 0 else {
                return nil
            }

            guard
                let typeRawValue = buffer.readInteger(as: UInt8.self),
                let type = Types(rawValue: typeRawValue)
            else { return nil }
            
            // skip message length
            buffer.moveReaderIndex(forwardBy: 4)
            
            switch type {
            case .hello:
                guard
                    let proto = readString(),
                    let version = readUInt32()
                else { return nil }
                self = .hello(protocol: proto, version: version)

            case .error:
                guard let msg = readString() else { return nil }
                self = .error(msg: msg)
            
            case .ok:
                self = .ok
                
            case .dialAuthority:
                guard let site = readUInt32() else { return nil }
                self = .dialAuthority(site: site)

            case .targetPopulations:
                guard
                    let site = readUInt32(),
                    let count = readUInt32()
                else { return nil }
                
                var populations: [TargetPopulation] = []
                
                for _ in 0..<count {
                    guard
                        let species = readString(),
                        let min = readUInt32(),
                        let max = readUInt32()
                    else { return nil }
                    
                    populations.append(TargetPopulation(species: species, min: min, max: max))
                }

                self = .targetPopulations(site: site, populations: populations)
                
            case .createPolicy:
                guard
                    let species = readString(),
                    let actionRawValue = readUInt8(),
                    let action = Action(rawValue: actionRawValue)
                else { return nil }
                
                self = .createPolicy(species: species, action: action)

            case .deletePolicy:
                guard let policy = readUInt32() else { return nil }
                self = .deletePolicy(policy: policy)
                
            case .policyResult:
                guard let policy = readUInt32() else { return nil }
                self = .policyResult(policy: policy)

            case .siteVisit:
                guard
                    let site = readUInt32(),
                    let count = readUInt32()
                else { return nil }
                
                var populations: [ObservedPopulation] = []
                
                for _ in 0..<count {
                    guard
                        let species = readString(),
                        let count = readUInt32()
                    else { return nil }
                    
                    populations.append(ObservedPopulation(species: species, count: count))
                }

                self = .siteVisit(site: site, populations: populations)
            }
            
            // checksum should be the only byte left in the buffer
            guard buffer.readableBytes == 1 else { return nil }
        }
        
        func write(out: inout ByteBuffer) {
            // leave space for message type code and length
            out.moveWriterIndex(forwardBy: 5)
            
            func write(_ str: String) {
                out.writeInteger(UInt32(str.utf8.count))
                out.writeBytes(str.utf8)
            }
            func write(_ u32: UInt32) { out.writeInteger(u32) }
            func write(_ u8: UInt8) { out.writeInteger(u8) }
            func setType(_ type: Types) { out.setInteger(type.rawValue, at: 0) }
            
            switch self {
            case let .hello(proto, version):
                setType(.hello)
                write(proto)
                write(version)
            case let .error(msg):
                setType(.error)
                write(msg)
            case .ok:
                setType(.ok)
            case let .dialAuthority(site):
                setType(.dialAuthority)
                write(site)
            case let .targetPopulations(site, populations):
                setType(.targetPopulations)
                write(site)
                write(UInt32(populations.count))
                for p in populations {
                    write(p.species)
                    write(p.min)
                    write(p.max)
                }
            case let .createPolicy(species, action):
                setType(.createPolicy)
                write(species)
                write(action.rawValue)
            case let .deletePolicy(policy):
                setType(.deletePolicy)
                write(policy)
            case let .policyResult(policy):
                setType(.policyResult)
                write(policy)
            case let .siteVisit(site, populations):
                setType(.siteVisit)
                write(site)
                write(UInt32(populations.count))
                for p in populations {
                    write(p.species)
                    write(p.count)
                }
            }
            
            // patch message with length prefix
            out.setInteger(UInt32(out.readableBytes + 1), at: 1)

            // calculate the checksum byte and add it at the end
            let checksum = UInt8(0 &- out.readableBytesView.reduce(0, { $0 &+ $1 }))
            out.writeInteger(checksum)
        }
    }

    final class Decoder: ByteToMessageDecoder {
        typealias InboundOut = Message?
        
        func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            guard
                let length = buffer.getInteger(at: buffer.readerIndex + 1, as: UInt32.self),
                var slice = buffer.readSlice(length: Int(length))
            else { return .needMoreData }
            
            let message = Message(buffer: &slice)
            context.fireChannelRead(wrapInboundOut(message))
            if let message = message {
                print("<-- \(message)")
            } else {
                print("<-- nil")
            }

            return .continue
        }
    }
    
    final class Encoder: MessageToByteEncoder {
        typealias OutboundIn = Message
        
        func encode(data: OutboundIn, out: inout ByteBuffer) throws {
            data.write(out: &out)
            print("--> \(data)")
        }
    }
    
    final class ServerHandler: ChannelInboundHandler {
        typealias InboundIn = Message?
        typealias OutboundOut = Message
        
        var completedHello = false
        let centralControl: CentralControl
        let continuation: AsyncStream<(UInt32, [String: UInt32])>.Continuation
        
        init(centralControl: CentralControl) {
            self.centralControl = centralControl
            
            var continuation: AsyncStream<(UInt32, [String: UInt32])>.Continuation!
            let stream = AsyncStream { continuation = $0 }
            self.continuation = continuation
            
            Task {
                for await (site, populations) in stream {
                    try await centralControl.siteVisit(site: site, populations: populations)
                }
            }
        }
        
        func channelActive(context: ChannelHandlerContext) {
            context.writeAndFlush(wrapOutboundOut(.standardHello), promise: nil)
        }
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            func mergeCounts(populations: [Message.ObservedPopulation]) -> [String: UInt32]? {
                var newCounts: [String: UInt32] = [:]
                for p in populations {
                    guard newCounts[p.species, default: p.count] == p.count else {
                        return nil
                    }
                    newCounts[p.species] = p.count
                }
                return newCounts
            }
            
            guard let request = unwrapInboundIn(data) else {
                context.writeAndFlush(wrapOutboundOut(.error(msg: "unrecognized request")), promise: nil)
                context.close(promise: nil)
                return
            }
            
            guard completedHello else {
                if request != .standardHello {
                    context.writeAndFlush(wrapOutboundOut(.error(msg: "incorrect hello message")), promise: nil)
                    context.close(promise: nil)
                }
                
                completedHello = true
                return
            }
            
            switch (request) {
            case let .siteVisit(site: site, populations: populations):
                if let populations = mergeCounts(populations: populations) {
                    continuation.yield((site, populations))
                } else {
                    context.writeAndFlush(wrapOutboundOut(.error(msg: "conflicting counts")), promise: nil)
                }

            default:
                context.writeAndFlush(wrapOutboundOut(.error(msg: "unexpected request \(request)")), promise: nil)
                context.close(promise: nil)
            }
        }
    }
    
    actor CentralControl {
        enum SiteEntry {
            case pending(Task<SiteControl, Error>)
            case ready(SiteControl)
        }
        
        var sites: [UInt32: SiteEntry] = [:]
        
        private func getSite(site siteId: UInt32) async throws -> SiteControl {
            if let entry = sites[siteId] {
                switch entry {
                case let .ready(site):
                    return site
                case let .pending(task):
                    return try await task.value
                }
            }
            
            let task = Task {
                try await SiteControl(site: siteId)
            }
            
            sites[siteId] = .pending(task)
            do {
                let site = try await task.value
                sites[siteId] = .ready(site)
                return site
            } catch {
                sites[siteId] = nil
                throw error
            }
        }
        
        func siteVisit(site siteId: UInt32, populations: [String: UInt32]) async throws {
            try await getSite(site: siteId).visit(populations: populations)
        }
    }

    actor SiteControl {
        let id: UInt32
        let continuation: AsyncStream<Void>.Continuation
        let asyncChannel: AsyncChannelIO
        var species: [String: (target: ClosedRange<UInt32>, policy: Policy?)]
        var latestCounts: [String: UInt32] = [:]
        
        struct Policy {
            let action: Message.Action
            let id: UInt32
        }
        
        struct AsyncChannelIO {
            let channel: Channel

            init(_ channel: Channel) {
                self.channel = channel
            }

            func start() async throws -> AsyncChannelIO {
                try await channel.pipeline.addHandler(RequestResponseHandler()).get()
                return self
            }

            func sendRequest(_ request: Message) async throws -> Message {
                let responsePromise: EventLoopPromise<Message> = channel.eventLoop.makePromise()
                try await self.channel.writeAndFlush((request, responsePromise)).get()
                return try await responsePromise.futureResult.get()
            }

            func close() async throws {
                try await self.channel.close()
            }
        }
        
        init(site id: UInt32) async throws {
            self.id = id
            
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            let channel = try await ClientBootstrap(group: group).connect(host: "pestcontrol.protohackers.com", port: 20547).get()
            try await channel.pipeline.addHandler(ByteToMessageHandler(Decoder())).get()
            try await channel.pipeline.addHandler(MessageToByteHandler(Encoder())).get()
            
            asyncChannel = try await AsyncChannelIO(channel).start()
            
            let response1 = try await asyncChannel.sendRequest(.standardHello)
            guard response1 == .standardHello else {
                _ = try await asyncChannel.sendRequest(.error(msg: "incorrect hello response"))
                try await asyncChannel.close()
                throw "bad hello response from authority server"
            }
            
            let response2 = try await asyncChannel.sendRequest(.dialAuthority(site: id))
            guard case let .targetPopulations(_, populations) = response2 else {
                _ = try await asyncChannel.sendRequest(.error(msg: "bad response to target populations"))
                try await asyncChannel.close()
                throw "bad target populations response from authority server"
            }
            
            species = [:]
            for p in populations {
                species[p.species] = (p.min...p.max, nil)
            }
            
            var continuation: AsyncStream<Void>.Continuation!
            let stream = AsyncStream { continuation = $0 }
            self.continuation = continuation
            
            Task {
                for await _ in stream {
                    for (speciesName, count) in latestCounts {
                        guard let (target, currentPolicy) = species[speciesName] else {
                            continue
                        }

                        let desiredAction: Message.Action?
                        if count > target.upperBound {
                            desiredAction = .cull
                        } else if count < target.lowerBound {
                            desiredAction = .conserve
                        } else {
                            desiredAction = nil
                        }
                        
                        if currentPolicy?.action == desiredAction {
                            continue
                        }
                        
                        if let policyId = currentPolicy?.id {
                            _ = try await asyncChannel.sendRequest(.deletePolicy(policy: policyId))
                            species[speciesName]?.policy = nil
                        }
                        
                        if let desiredAction = desiredAction {
                            let response = try await asyncChannel.sendRequest(
                                .createPolicy(species: speciesName, action: desiredAction)
                            )
                            if case let .policyResult(id) = response {
                                species[speciesName]?.policy = Policy(action: desiredAction, id: id)
                            } else {
                                print("*** unexpected response received to create policy \(response)")
                            }
                        }
                    }
                }
            }
        }
        
        func visit(populations: [String: UInt32]) {
            latestCounts.merge(
                species.keys.map { ($0, populations[$0] ?? 0) },
                uniquingKeysWith: { _, s in s }
            )
            continuation.yield()
        }
    }
    
    // Adapted from swift-nio-extras
    public final class RequestResponseHandler: ChannelDuplexHandler {
        public typealias InboundIn = Message
        public typealias InboundOut = Never
        public typealias OutboundIn = (Message, EventLoopPromise<Message>)
        public typealias OutboundOut = Message

        private enum State {
            case operational
            case error(Error)

            var isOperational: Bool {
                switch self {
                case .operational:
                    return true
                case .error:
                    return false
                }
            }
        }

        private var state: State = .operational
        private var promiseBuffer: CircularBuffer<EventLoopPromise<Message>>

        public init(initialBufferCapacity: Int = 4) {
            self.promiseBuffer = CircularBuffer(initialCapacity: initialBufferCapacity)
        }

        public func channelInactive(context: ChannelHandlerContext) {
            switch self.state {
            case .error:
                // We failed any outstanding promises when we entered the error state and will fail any
                // new promises in write.
                assert(self.promiseBuffer.count == 0)
            case .operational:
                let promiseBuffer = self.promiseBuffer
                self.promiseBuffer.removeAll()
                promiseBuffer.forEach { promise in
                    promise.fail(ChannelError.eof)
                }
            }
            context.fireChannelInactive()
        }

        public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            guard self.state.isOperational else {
                // we're in an error state, ignore further responses
                assert(self.promiseBuffer.count == 0)
                return
            }

            let response = self.unwrapInboundIn(data)
            let promise = self.promiseBuffer.removeFirst()

            promise.succeed(response)
        }

        public func errorCaught(context: ChannelHandlerContext, error: Error) {
            guard self.state.isOperational else {
                assert(self.promiseBuffer.count == 0)
                return
            }
            self.state = .error(error)
            let promiseBuffer = self.promiseBuffer
            self.promiseBuffer.removeAll()
            context.close(promise: nil)
            promiseBuffer.forEach {
                $0.fail(error)
            }
        }

        public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            let (request, responsePromise) = self.unwrapOutboundIn(data)
            switch self.state {
            case .error(let error):
                assert(self.promiseBuffer.count == 0)
                responsePromise.fail(error)
                promise?.fail(error)
            case .operational:
                self.promiseBuffer.append(responsePromise)
                context.write(self.wrapOutboundOut(request), promise: promise)
            }
        }
    }
}
