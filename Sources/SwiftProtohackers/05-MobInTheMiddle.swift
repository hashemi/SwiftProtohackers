//
//  05-MobInTheMiddle.swift
//  
//
//  Created by Ahmad Alhashemi on 06/01/2023.
//

import NIOCore
import NIOPosix


final class MobInTheMiddleHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private enum Status {
        case attached(peer: Channel)
        case detached(buffers: [ByteBuffer])
    }
    
    private var status: Status
    
    init(peer: Channel? = nil) {
        if let peer = peer {
            self.status = .attached(peer: peer)
        } else {
            self.status = .detached(buffers: [])
        }
    }
    
    func channelActive(context: ChannelHandlerContext) {
        let channel = context.channel
        if case .detached(_) = status {
            ClientBootstrap(group: context.eventLoop)
                .channelInitializer { clientChannel in
                    clientChannel.pipeline.addHandler(ByteToMessageHandler(MessagePerLineDecoder())).flatMap { v in
                        clientChannel.pipeline.addHandler(MobInTheMiddleHandler(peer: channel))
                    }
                }.connect(host: "chat.protohackers.com", port: 16963)
                .whenSuccess { clientChannel in
                    if case let .detached(storedBuffers) = self.status {
                        storedBuffers.forEach { channel.write($0, promise: nil) }
                        channel.flush()
                    }
                    self.status = .attached(peer: clientChannel)
                }
        }
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        if case let .attached(channel) = status {
            channel.close(mode: .all, promise: nil)
        }
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        var modified = context.channel.allocator.buffer(capacity: buffer.readableBytes)

        guard let message = buffer.readString(length: buffer.readableBytes) else {
            return
        }

        for part in message.split(separator: " ") {
            let lastPart = part.hasSuffix("\n")
            if part.prefixMatch(of: #/^7[a-zA-Z0-9]{25,34}\n?$/#) != nil {
                modified.writeStaticString("7YWHMfk9JZe0LM0g1ZauHuiSxhI")
                if lastPart {
                    modified.writeInteger(UInt8(ascii: "\n"))
                }
            } else {
                modified.writeSubstring(part)
            }
            if !lastPart {
                modified.writeInteger(UInt8(ascii: " "))
            }
        }

        switch status {
        case .attached(let peer):
            peer.writeAndFlush(modified, promise: nil)
        case .detached(var buffers):
            buffers.append(modified)
            status = .detached(buffers: buffers)
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: \(error)")
        context.close(promise: nil)
    }
}
