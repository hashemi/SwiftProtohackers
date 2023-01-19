import XCTest
@testable import SwiftProtohackers

final class SwiftProtohackersTests: XCTestCase {
    func testEncryption1() {
        let original: [UInt8] = [
            0x68, 0x65, 0x6c, 0x6c, 0x6f
        ]
        let cipherSpec: [InsecureSocketsLayer.CipherOp] = [.xor(1), .reversebits]
        let encrypted = original.encrypt(cipherSpec: cipherSpec, posOffset: 0)
        let decrypted = encrypted.decrypt(cipherSpec: cipherSpec, posOffset: 0)
        XCTAssertEqual(encrypted, [0x96, 0x26, 0xb6, 0xb6, 0x76])
        XCTAssertEqual(decrypted, original)
    }
    
    func testEncryption2() {
        let original: [UInt8] = [
            0x68, 0x65, 0x6c, 0x6c, 0x6f
        ]
        let cipherSpec: [InsecureSocketsLayer.CipherOp] = [.xor(1), .reversebits, .addpos]
        let encrypted = original.encrypt(cipherSpec: cipherSpec, posOffset: 0)
        let decrypted = encrypted.decrypt(cipherSpec: cipherSpec, posOffset: 0)
        XCTAssertEqual(encrypted, [0x96, 0x27, 0xb8, 0xb9, 0x7a])
        XCTAssertEqual(decrypted, original)
    }
    
    func testEncryption3() {
        let cipherSpec: [InsecureSocketsLayer.CipherOp] = .init(bytes: [0x3])
        XCTAssertTrue(cipherSpec.isNoOpCipher)
    }
}
