//
//  01-PrimeTime.swift
//  
//
//  Created by Ahmad Alhashemi on 03/01/2023.
//

import Foundation
import NIOCore
import NIOFoundationCompat

func isPrime(_ number: Double) -> Bool {
    if !number.isFinite { return false }
    if number >= Double(Int.max) || number <= Double(Int.min) { return false }

    let int = Int(number)
    
    if number != Double(int) { return false }
    if int <= 1 { return false }
    if int <= 3 { return true }
    
    if int % 2 == 0 || int % 3 == 0 { return false }
    
    var i = 5
    while i * i <= int {
        if int % i == 0 || int % (i + 2) == 0 { return false }
        i += 6
    }
    
    return true
}

final class MessagePerLineDecoder: ByteToMessageDecoder {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let idx = buffer.withUnsafeReadableBytes({ $0.firstIndex(of: UInt8(ascii: "\n")) }) else {
            return .needMoreData
        }
        
        context.fireChannelRead(wrapInboundOut(buffer.readSlice(length: idx + 1)!))
        
        return .continue
    }
}

final class PrimeTimeHandler: ChannelInboundHandler {
    private struct Request: Decodable {
        let method: String
        let number: Double
        
        var response: Response {
            Response(prime: isPrime(number))
        }
    }

    private struct Response: Encodable {
        let method = "isPrime"
        let prime: Bool
    }

    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var inBuffer = unwrapInboundIn(data)
        var outBuffer = ByteBuffer()
        
        if
            let request = try? inBuffer.readJSONDecodable(Request.self, length: inBuffer.readableBytes),
            request.method == "isPrime" {
            try! outBuffer.writeJSONEncodable(request.response)
        } else {
            outBuffer.writeString("{}")
        }

        outBuffer.writeBytes([UInt8(ascii: "\n")])
        context.writeAndFlush(wrapInboundOut(outBuffer), promise: nil)
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: \(error)")
        context.close(promise: nil)
    }
}
