//
//  02-MeansToAnEnd.swift
//  
//
//  Created by Ahmad Alhashemi on 03/01/2023.
//

import NIOCore
import NIOPosix

enum Message {
    case insert(timestamp: Int32, price: Int32)
    case query(mintime: Int32, maxtime: Int32)
    
    init?(rawValue: (UInt8, Int32, Int32)) {
        switch rawValue.0 {
        case UInt8(ascii: "I"):
            self = .insert(timestamp: rawValue.1, price: rawValue.2)
        case UInt8(ascii: "Q"):
            self = .query(mintime: rawValue.1, maxtime: rawValue.2)
        default:
            return nil
        }
    }
}

final class InvestmentBankMessageDecoder: ByteToMessageDecoder {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = Message
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let raw = buffer.readMultipleIntegers(as: (UInt8, Int32, Int32).self) else {
            return .needMoreData
        }
        
        if let message = Message(rawValue: raw) {
            context.fireChannelRead(wrapInboundOut(message))
        } else {
            print("Unexpected message received: \(raw)")
            context.close(promise: nil)
        }
        
        return .continue
    }
}

final class MeansToAnEndHandler: ChannelInboundHandler {
    private var prices: [(timestamp: Int32, price: Int32)] = []

    typealias InboundIn = Message
    typealias InboundOut = ByteBuffer
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case let .insert(timestamp: timestamp, price: price):
            prices.append((timestamp, price))
        case let .query(mintime: mintime, maxtime: maxtime):
            let filtered = prices
                .filter { mintime <= $0.timestamp && $0.timestamp <= maxtime }
                .map(\.price)
            
            let avg: Int32
            if filtered.isEmpty {
                avg = 0
            } else {
                avg = Int32(filtered.map(Double.init).reduce(0, +) / Double(filtered.count))
            }
            
            let buffer = ByteBuffer(integer: avg)
            context.writeAndFlush(wrapInboundOut(buffer), promise: nil)
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: \(error)")
        context.close(promise: nil)
    }
}
