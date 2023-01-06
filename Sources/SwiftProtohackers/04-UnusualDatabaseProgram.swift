//
//  04-UnusualDatabaseProgram.swift
//  
//
//  Created by Ahmad Alhashemi on 06/01/2023.
//

import NIOCore

actor UnusualDatabase {
    enum Operation {
        case insert(key: String, value: String)
        case retrieve(key: String)
        
        init(rawValue s: String) {
            if let eqIdx = s.firstIndex(of: "=") {
                self = .insert(key: String(s[..<eqIdx]), value: String(s[eqIdx...].dropFirst()))
            } else {
                self = .retrieve(key: s)
            }
        }
    }

    private var data: [String: String] = ["version": "SwiftProtohackers 1.0"]
    
    func perform(_ op: Operation) -> String? {
        print(op)
        switch op {
        case let .insert(key, value):
            if key != "version" {
                data[key] = value
            }
            return nil
        case let .retrieve(key):
            return "\(key)=\(data[key] ?? "")"
        }
    }
}

final class UnusualDatabaseMessageDecoder: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias InboundOut = AddressedEnvelope<UnusualDatabase.Operation>
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let envelope = unwrapInboundIn(data)
        var buffer = envelope.data
        
        guard let message = buffer.readString(length: buffer.readableBytes) else {
            print("Error: invalid string received")
            return
        }
        
        let operation = UnusualDatabase.Operation(rawValue: message)
        
        context.fireChannelRead(
            wrapInboundOut(AddressedEnvelope(remoteAddress: envelope.remoteAddress, data: operation))
        )
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error:", error)
        context.close(promise: nil)
    }
}

final class UnusualDatabaseMessageHandler: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<UnusualDatabase.Operation>
    typealias OutboundOut = AddressedEnvelope<ByteBuffer>
    
    let database = UnusualDatabase()
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let request = unwrapInboundIn(data)
        let channel = context.channel

        Task {
            if let response = await database.perform(request.data), channel.isWritable {
                let buffer = channel.allocator.buffer(string: response)
                let envelope = AddressedEnvelope(
                    remoteAddress: request.remoteAddress,
                    data: buffer
                )
                channel.writeAndFlush(envelope, promise: nil)
            }
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error:", error)
        context.close(promise: nil)
    }
}
