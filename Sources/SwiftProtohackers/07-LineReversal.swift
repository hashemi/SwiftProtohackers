//
//  07-LineReversal.swift
//  
//
//  Created by Ahmad Alhashemi on 13/01/2023.
//

import NIOCore

enum LineReversal {
    struct Message: Hashable, CustomStringConvertible {
        enum Payload: Hashable, CustomStringConvertible {
            case connect
            case data(Int, [UInt8])
            case ack(Int)
            case close
            
            var description: String {
                switch self {
                case .connect: return ".connect"
                case let .data(pos, data): return ".data(\(pos), \(data.count))"
                case let .ack(pos): return ".ack(\(pos))"
                case .close: return ".close"
                }
            }
        }
        
        let session: Int
        let payload: Payload
        
        init(session: Int, payload: Payload) {
            self.session = session
            self.payload = payload
        }
        
        init?(bytes: [UInt8]) {
            guard bytes.count >= 7 && bytes.first == .forwardSlash && bytes.last == .forwardSlash
            else {
                print("*** invalid datagram \(bytes)")
                return nil
            }
            
            let parts = bytes.split(separator: .forwardSlash, maxSplits: 3)
            
            guard
                parts.count > 1,
                let sessionStr = String(bytes: parts[1], encoding: .ascii),
                let sessionInt = Int(sessionStr),
                sessionInt >= 0
            else {
                return nil
            }
            self.session = sessionInt
            
            switch (Array<UInt8>(parts[0]), parts.count) {
            case (Array<UInt8>("connect".utf8), 2):
                self.payload = .connect
                
            case (Array<UInt8>("data".utf8), 4):
                guard
                    let posStr = String(bytes: parts[2], encoding: .ascii),
                    let pos = Int(posStr),
                    let data = Array<UInt8>(parts[3].dropLast()).unescaped,
                    pos >= 0
                else {
                    return nil
                }
                self.payload = .data(pos, data)
                
            case (Array<UInt8>("ack".utf8), 3):
                guard
                    let posStr = String(bytes: parts[2], encoding: .ascii),
                    let pos = Int(posStr)
                else {
                    return nil
                }
                self.payload = .ack(pos)
                
            case (Array<UInt8>("close".utf8), 2):
                self.payload = .close
                
            default:
                return nil
            }
        }
        
        var rawBytes: [UInt8] {
            switch payload {
            case .connect:
                return Array("/connect/\(session)/".utf8)
            case let .data(pos, data):
                return Array("/data/\(session)/\(pos)/".utf8) + data.escaped + [.forwardSlash]
            case let .ack(length):
                return Array("/ack/\(session)/\(length)/".utf8)
            case .close:
                return Array("/close/\(session)/".utf8)
            }
        }
        
        var description: String {
            return "\(session): \(payload)"
        }
    }
    
    final class EncoderDecoder: ChannelDuplexHandler {
        typealias InboundIn = AddressedEnvelope<ByteBuffer>
        typealias InboundOut = AddressedEnvelope<Message>
        typealias OutboundIn = AddressedEnvelope<Message>
        typealias OutboundOut = AddressedEnvelope<ByteBuffer>
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let envelope = unwrapInboundIn(data)
            var buffer = envelope.data
            
            guard let bytes = buffer.readBytes(length: buffer.readableBytes) else {
                print("*** cannot read bytes off of the buffer")
                return
            }
            
            guard let message = Message(bytes: bytes) else {
                print("*** cannot decode message: \(String(bytes: bytes, encoding: .ascii)?.debugDescription ?? bytes.debugDescription)")
                return
            }
            
            print("<-- \(message)")
            
            context.fireChannelRead(
                wrapInboundOut(
                    AddressedEnvelope(
                        remoteAddress: envelope.remoteAddress,
                        data: message
                    )
                    
                )
            )
        }
        
        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            let envelope = unwrapOutboundIn(data)
            let bytes = envelope.data.rawBytes

            print("--> \(envelope.data)")
            
            context.write(
                wrapOutboundOut(
                    AddressedEnvelope(
                        remoteAddress: envelope.remoteAddress,
                        data: ByteBuffer(bytes: bytes)
                    )
                ),
                promise: promise
            )
        }
    }
    
    final class Session {
        let id: Int
        let addr: SocketAddress
        private(set) var connected = false
        private var inBuffer = ByteBuffer()
        private var outBuffer = ByteBuffer()
        private var lastReceive = NIODeadline.now()
        private var lastDataSend = NIODeadline.now()

        init(id: Int, addr: SocketAddress) {
            self.id = id
            self.addr = addr
        }
        
        func receive(_ payload: Message.Payload) -> [Message.Payload] {
            lastReceive = .now()
            switch (payload, connected) {
            case (.connect, _):
                connected = true
                return [.ack(inBuffer.writerIndex)]

            case let (.data(pos, data), true):
                if pos <= inBuffer.writerIndex {
                    let offset = inBuffer.writerIndex - pos
                    if offset < data.count {
                        inBuffer.writeBytes(data[offset...])
                    }
                }

                return [.ack(inBuffer.writerIndex)] + pendingDataPayload()

            case let (.ack(pos), true):
                // the peer misbehaving, close the session
                guard pos <= outBuffer.writerIndex else {
                    connected = false
                    return [.close]
                }

                if pos > outBuffer.readerIndex {
                    outBuffer.moveReaderIndex(to: pos)
                }
                
                return pendingDataPayload()
                
            case (.close, true):
                return [.close]
                
            case (_, false):
                connected = false
                return [.close]
            }
        }
        
        func ping() -> [Message.Payload] {
            if NIODeadline.now() - lastReceive > .seconds(60) {
                connected = false
                print("*** \(id) timeout")
                return [.close]
            }
            
            if NIODeadline.now() - lastDataSend > .seconds(3) {
                return pendingDataPayload()
            }
            
            // wait for next ping
            return []
        }
        
        private func pendingDataPayload() -> [Message.Payload] {
            reverse()
            
            if outBuffer.readableBytes > 0 {
                lastDataSend = .now()
                let data = outBuffer.getBytes(at: outBuffer.readerIndex, length: outBuffer.readableBytes)!
                return stride(from: 0, to: data.count, by: 400).map {
                    Message.Payload.data(outBuffer.readerIndex + $0, Array(data[$0..<min($0 + 400, data.count)]))
                }
            }

            return []
        }
        
        private func reverse() {
            guard let idx = inBuffer.withUnsafeReadableBytes({ $0.firstIndex(of: .newline)}) else {
                return
            }
            
            let line = inBuffer.readBytes(length: idx)!
            inBuffer.moveReaderIndex(forwardBy: 1) // skip over the newline
            
            outBuffer.writeBytes(line.reversed())
            outBuffer.writeInteger(UInt8.newline)
            
            reverse()
        }
    }
    
    final class LRCPHandler: ChannelInboundHandler {
        typealias InboundIn = AddressedEnvelope<Message>
        typealias OutboundOut = AddressedEnvelope<Message>
        
        var sessions: [Int: Session] = [:]
        
        func processSessionResponse(context: ChannelHandlerContext, session: Session, response: [Message.Payload]) {
            for payload in response {
                context.write(
                    self.wrapOutboundOut(
                        AddressedEnvelope(
                            remoteAddress: session.addr,
                            data: Message(
                                session: session.id,
                                payload: payload
                            )
                        )
                    )
                    , promise: nil)
            }
            
            context.flush()
            
            if response.contains(.close) {
                sessions.removeValue(forKey: session.id)
            }
        }
        
        func channelActive(context: ChannelHandlerContext) {
            context.eventLoop.scheduleRepeatedTask(initialDelay: .seconds(1), delay: .seconds(1)) { _ in
                for session in self.sessions.values {
                    let response = session.ping()
                    self.processSessionResponse(
                        context: context,
                        session: session,
                        response: response
                    )
                }
            }
        }
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let envelope = unwrapInboundIn(data)
            
            if !sessions.keys.contains(envelope.data.session) {
                sessions[envelope.data.session] = Session(
                    id: envelope.data.session,
                    addr: envelope.remoteAddress
                )
            }
            
            let session = sessions[envelope.data.session]!
            
            let response = session.receive(envelope.data.payload)
            
            self.processSessionResponse(
                context: context,
                session: session,
                response: response
            )
        }
    }
}

fileprivate extension UInt8 {
    static let forwardSlash = UInt8(ascii: "/")
    static let backSlash = UInt8(ascii: "\\")
    static let newline = UInt8(ascii: "\n")
}

fileprivate extension Array where Element == UInt8 {
    var escaped: [UInt8] {
        var ret: [UInt8] = []
        for c in self {
            if c == .backSlash || c == .forwardSlash {
                ret.append(.backSlash)
            }
            ret.append(c)
        }
        return ret
    }
    
    var unescaped: [UInt8]? {
        var ret: [UInt8] = []
        var escapeSequence = false
        for c in self {
            if escapeSequence {
                if (c == .forwardSlash || c == .backSlash) {
                    escapeSequence = false
                } else {
                    print("Cannot unescape: \(self.debugDescription)")
                    // invalid data
                    return nil
                }
            } else {
                if (c == .backSlash) {
                    escapeSequence = true
                    continue
                }
                if (c == .forwardSlash) {
                    // unescaped forward slash
                    return nil
                }
            }

            ret.append(c)
        }
        
        // backslash terminated string
        if escapeSequence {
            print("Cannot unescape: \(self.debugDescription)")
            return nil
        }
        
        return ret
    }
}
