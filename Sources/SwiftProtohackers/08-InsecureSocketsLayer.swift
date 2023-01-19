//
//  08-InsecureSocketsLayer.swift
//  
//
//  Created by Ahmad Alhashemi on 18/01/2023.
//

import NIOCore

fileprivate let bitSwapped: [UInt8] = [
    0, 128, 64, 192, 32, 160, 96, 224, 16, 144, 80, 208, 48, 176, 112, 240,
    8, 136, 72, 200, 40, 168, 104, 232, 24, 152, 88, 216, 56, 184, 120, 248,
    4, 132, 68, 196, 36, 164, 100, 228, 20, 148, 84, 212, 52, 180, 116, 244,
    12, 140, 76, 204, 44, 172, 108, 236, 28, 156, 92, 220, 60, 188, 124, 252,
    2, 130, 66, 194, 34, 162, 98, 226, 18, 146, 82, 210, 50, 178, 114, 242,
    10, 138, 74, 202, 42, 170, 106, 234, 26, 154, 90, 218, 58, 186, 122, 250,
    6, 134, 70, 198, 38, 166, 102, 230, 22, 150, 86, 214, 54, 182, 118, 246,
    14, 142, 78, 206, 46, 174, 110, 238, 30, 158, 94, 222, 62, 190, 126, 254,
    1, 129, 65, 193, 33, 161, 97, 225, 17, 145, 81, 209, 49, 177, 113, 241,
    9, 137, 73, 201, 41, 169, 105, 233, 25, 153, 89, 217, 57, 185, 121, 249,
    5, 133, 69, 197, 37, 165, 101, 229, 21, 149, 85, 213, 53, 181, 117, 245,
    13, 141, 77, 205, 45, 173, 109, 237, 29, 157, 93, 221, 61, 189, 125, 253,
    3, 131, 67, 195, 35, 163, 99, 227, 19, 147, 83, 211, 51, 179, 115, 243,
    11, 139, 75, 203, 43, 171, 107, 235, 27, 155, 91, 219, 59, 187, 123, 251,
    7, 135, 71, 199, 39, 167, 103, 231, 23, 151, 87, 215, 55, 183, 119, 247,
    15, 143, 79, 207, 47, 175, 111, 239, 31, 159, 95, 223, 63, 191, 127, 255
]

fileprivate extension UInt8 {
    func encrypt(_ op: InsecureSocketsLayer.CipherOp, pos: UInt8) -> UInt8 {
        switch op {
        case .reversebits: return bitSwapped[Int(self)]
        case .xor(let n): return self ^ n
        case .xorpos: return self ^ pos
        case .add(let n): return self &+ n
        case .addpos: return self &+ pos
        }
    }
    
    func decrypt(_ op: InsecureSocketsLayer.CipherOp, pos: UInt8) -> UInt8 {
        switch op {
        case .reversebits: return bitSwapped[Int(self)]
        case .xor(let n): return self ^ n
        case .xorpos: return self ^ pos
        case .add(let n): return self &- n
        case .addpos: return self &- pos
        }
    }
    
    func encrypt(cipherSpec: [InsecureSocketsLayer.CipherOp], pos: UInt8) -> UInt8 {
        cipherSpec.reduce(self) {
            $0.encrypt($1, pos: pos)
        }
    }
    
    func decrypt(cipherSpec: [InsecureSocketsLayer.CipherOp], pos: UInt8) -> UInt8 {
        cipherSpec.reversed().reduce(self) {
            $0.decrypt($1, pos: pos)
        }
    }
}

extension Array where Element == InsecureSocketsLayer.CipherOp {
    init(bytes: [UInt8]) {
        precondition(!bytes.isEmpty)
        precondition(bytes.last == 0x00)
        self.init()
        var rem = bytes[...]
        while true {
            switch rem.removeFirst() {
            case 0x01: append(.reversebits)
            case 0x02: append(.xor(rem.removeFirst()))
            case 0x03: append(.xorpos)
            case 0x04: append(.add(rem.removeFirst()))
            case 0x05: append(.addpos)
            case 0x00 where rem.isEmpty: return
            default:
                fatalError("Unrecognized op code")
            }
        }
    }
    
    var isNoOpCipher: Bool {
        Array<UInt8>(0...UInt8.max) == Array<UInt8>(0...UInt8.max).encrypt(cipherSpec: self, posOffset: 0)
    }
}

extension RandomAccessCollection where Element == UInt8 {
    func encrypt(cipherSpec: [InsecureSocketsLayer.CipherOp], posOffset: Int) -> [UInt8] {
        Array(self.enumerated().map({ (idx, byte) in
            byte.encrypt(cipherSpec: cipherSpec, pos: UInt8((idx + posOffset) % 256))
        }))
    }
    
    func decrypt(cipherSpec: [InsecureSocketsLayer.CipherOp], posOffset: Int) -> [UInt8] {
        Array(self.enumerated().map({ (idx, byte) in
            byte.decrypt(cipherSpec: cipherSpec, pos: UInt8((idx + posOffset) % 256))
        }))
    }
    
    var isCompleteRawCipherSpec: Bool {
        if last != 0 { return false }
        if count == 1 { return true }
        if self[index(endIndex, offsetBy: -2)] == 0x02 || self[index(endIndex, offsetBy: -2)] == 0x04 {
            return false
        }
        return true
    }
}


enum InsecureSocketsLayer {
    enum CipherOp {
        case reversebits
        case xor(UInt8)
        case xorpos
        case add(UInt8)
        case addpos
    }
    
    final class EncryptDecryptHandler: ChannelDuplexHandler {
        typealias InboundIn = ByteBuffer
        typealias InboundOut = ByteBuffer
        typealias OutboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        enum Status {
            case spec
            case stream
        }
        
        private var status = Status.spec
        private var rawCipherSpec: [UInt8] = []
        private var cipherSpec: [CipherOp] = []
        
        private var readPos = 0
        private var writePos = 0
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            var buffer = unwrapInboundIn(data)
            var requestData = buffer.readBytes(length: buffer.readableBytes)![...]
            
            if status == .spec {
                while !requestData.isEmpty && !rawCipherSpec.isCompleteRawCipherSpec {
                    if let nullIdx = requestData.firstIndex(of: 0x00) {
                        rawCipherSpec.append(contentsOf: requestData[...nullIdx])
                        requestData = requestData[(nullIdx + 1)...]
                    } else {
                        rawCipherSpec.append(contentsOf: requestData)
                        requestData.removeAll()
                    }
                }
                
                if rawCipherSpec.isCompleteRawCipherSpec {
                    status = .stream
                    print("*** Raw cipher bytes \(rawCipherSpec.debugDescription)")
                    cipherSpec = Array<CipherOp>(bytes: rawCipherSpec)
                    print("*** Cipher spec: \(cipherSpec)" + (cipherSpec.isNoOpCipher ? " [noop]" : ""))
                    if cipherSpec.isNoOpCipher {
                        context.close(promise: nil)
                        return
                    }
                }
            }

            if status == .stream {
                let decrypted = requestData.decrypt(cipherSpec: cipherSpec, posOffset: readPos)
                print("== using cipher spec \(cipherSpec)")
                print("== \(requestData.debugDescription) --> \(decrypted.debugDescription)")
                let out = context.channel.allocator.buffer(bytes: decrypted)
                
                context.fireChannelRead(wrapInboundOut(out))
                readPos += requestData.count
            }
        }
        
        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            var buffer = unwrapOutboundIn(data)
            let cleartext = buffer.readBytes(length: buffer.readableBytes)!
            let encrypted = cleartext.encrypt(cipherSpec: cipherSpec, posOffset: writePos)
            let out = context.channel.allocator.buffer(bytes: encrypted)
            
            writePos += encrypted.count
            context.write(wrapOutboundOut(out), promise: promise)
        }
    }
    
    final class AppHandler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            func itemCount(_ item: Substring) -> Int {
                return Int(item.split(separator: "x")[0])!
            }

            var buffer = unwrapInboundIn(data)
            let str = buffer.readString(length: buffer.readableBytes)!
            print("<-- \(str)")
            for line in str.split(separator: "\n") {
                let items = line.split(separator: ",")
                guard !items.isEmpty else { continue }
                var largestItem = items[0]
                for item in items[1...] {
                    if itemCount(item) > itemCount(largestItem) {
                        largestItem = item
                    }
                }
                print("--> \(largestItem)")
                context.write(
                    wrapOutboundOut(
                        ByteBuffer(string: String(largestItem) + "\n")
                    ),
                    promise: nil
                )
            }
            context.flush()
        }
    }
}
