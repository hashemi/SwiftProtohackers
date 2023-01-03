//
//  01-SmokeTest.swift
//  
//
//  Created by Ahmad Alhashemi on 02/01/2023.
//

import NIOCore
import NIOPosix

final class SmokeTestHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.write(data, promise: nil)
    }
    
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error:", error)
        context.close(promise: nil)
    }
}
