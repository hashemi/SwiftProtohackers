//
//  10-VoraciousCodeStorage.swift
//  
//
//  Created by Ahmad Alhashemi on 20/01/2023.
//

import NIOCore

enum VoraciousCodeStorage {
    enum Request {
        case help
        case get(filename: String, revision: Int?)
        case put(filename: String, content: String)
        case list(dir: String)
        case illegalMethod(method: String)
        case invalid(message: String)
    }
    
    enum Response {
        case ready
        case ok(String)
        case string(String)
        case error(String)
    }
    
    final class RequestDecoder: ByteToMessageDecoder {
        typealias InboundOut = Request
        
        enum Status {
            case command
            case putData(filename: String, length: Int)
        }

        var status = Status.command
        
        func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            func fire(_ r: Request) {
                context.fireChannelRead(wrapInboundOut(r))
            }

            switch status {
            case .command:
                guard let idx = buffer.withUnsafeReadableBytes({ $0.firstIndex(of: UInt8(ascii: "\n")) }) else {
                    return .needMoreData
                }
                
                let line = buffer.readString(length: idx, encoding: .ascii)!
                print("<-- \(line)")
                buffer.moveReaderIndex(forwardBy: 1) // skip \n

                let parts = line.split(separator: " ")
                if parts.isEmpty {
                    fire(.illegalMethod(method: ""))
                    return .continue
                }
                
                switch parts[0].uppercased() {
                case "HELP":
                    fire(.help)

                case "GET" where parts.count == 2 && parts[1].validFileName:
                    fire(.get(filename: String(parts[1]), revision: nil))
                case "GET" where parts.count == 2:
                    fire(.invalid(message: "illegal file name"))
                case "GET" where parts.count == 3 && parts[1].validFileName:
                    if let revision = parts[2].revision {
                        fire(.get(filename: String(parts[1]), revision: revision))
                    } else {
                        fire(.invalid(message: "no such revision"))
                    }
                case "GET" where parts.count == 3:
                    fire(.invalid(message: "illegal file name"))
                case "GET":
                    fire(.invalid(message: "usage: GET file [revision]"))

                case "PUT" where parts.count == 3 && parts[1].validFileName:
                    status = .putData(filename: String(parts[1]), length: Int(parts[2]) ?? 0)
                case "PUT" where parts.count == 3:
                    fire(.invalid(message: "illegal file name"))
                case "PUT":
                    fire(.invalid(message: "usage: PUT file length newline data"))

                case "LIST" where parts.count == 2 && parts[1].validDirName:
                    fire(.list(dir: String(parts[1].canonicalDirName)))
                case "LIST" where parts.count == 2:
                    fire(.invalid(message: "invalid dir name"))
                case "LIST":
                    fire(.invalid(message: "usage: LIST dir"))

                default:
                    fire(.illegalMethod(method: String(parts[0])))
                }

                return .continue

            case let .putData(filename, size):
                if buffer.readableBytes < size {
                    return .needMoreData
                }
                
                let data = buffer.readBytes(length: size)!
                
                if
                    let content = String(bytes: data, encoding: .ascii),
                    content.utf8.allSatisfy(\.isTextChar)
                {
                    fire(.put(filename: filename, content: content))
                } else {
                    print(data)
                    fire(.invalid(message: "text files only"))
                }

                status = .command
                return .continue
            }
        }
    }
    
    final class ResponseEncoder: MessageToByteEncoder {
        typealias OutboundIn = Response
        
        func encode(data: OutboundIn, out: inout ByteBuffer) throws {
            if case .string(_) = data { } else {
                print("--> \(data)")
            }
            
            switch data {
            case .ready: out.writeString("READY\n")
            case let .ok(message): out.writeString("OK \(message)\n")
            case let .string(string): out.writeString(string)
            case let .error(message: message): out.writeString("ERR \(message)\n")
            }
        }
    }
    
    actor Folder {
        enum Item {
            case folder(String)
            case file(String, Int)
        }
        
        private var folders: [String: Folder] = [:]
        private(set) var files: [String: File] = [:]
        
        private var folderNames: [String] = []
        private var fileNames: [String] = []
        
        func put(path: some RandomAccessCollection<Substring>, content: String) async -> Int {
            let fname = String(path.first!)
            if path.count == 1 {
                if !files.keys.contains(fname) {
                    fileNames.append(fname)
                    files[fname] = File()
                }
                return await files[fname]!.put(content)
            } else {
                if !folders.keys.contains(fname) {
                    folderNames.append(fname)
                    folders[fname] = Folder()
                }
                return await folders[fname]!.put(path: path.dropFirst(), content: content)
            }
        }
        
        func get(path: some RandomAccessCollection<Substring>) async -> Folder? {
            path.isEmpty ? self : await folders[String(path.first!)]?.get(path: path.dropFirst())
        }
        
        func list() async -> [Item] {
            var ret = folderNames.map(Item.folder)
            for name in fileNames {
                ret.append(.file(name, await files[name]!.latest))
            }
            return ret
        }
    }
    
    actor File {
        var revisions: [String] = []
        
        var latest: Int { revisions.count + 1 }
        
        func put(_ content: String) -> Int {
            if content != revisions.last {
                revisions.append(content)
            }
            return revisions.count
        }
        
        func get(revision: Int?) -> String? {
            let revision = revision ?? revisions.count
            guard (1...revisions.count).contains(revision) else { return nil }
            
            return revisions[revision - 1]
        }
    }
    
    final class Handler: ChannelInboundHandler {
        typealias InboundIn = Request
        typealias OutboundOut = Response
        
        func channelActive(context: ChannelHandlerContext) {
            context.writeAndFlush(wrapOutboundOut(.ready), promise: nil)
        }
        
        let root: Folder
        
        init(root: Folder) {
            self.root = root
        }
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let channel = context.channel
            @Sendable func write(_ r: Response) { channel.writeAndFlush(wrapOutboundOut(r), promise: nil) }

            let request = unwrapInboundIn(data)
            switch request {
            case .help:
                write(.ok("usage: HELP|GET|PUT|LIST"))
                write(.ready)

            case .put(let filename, let content):
                guard filename.first == "/" && filename.last != "/" else {
                    write(.error("illegal filename"))
                    break
                }

                Task {
                    let version = await root.put(path: filename.split(separator: "/"), content: content)
                    write(.ok("r\(version)"))
                    write(.ready)
                }

            case .get(let filename, let revision):
                Task {
                    let parts = filename.dropFirst().split(separator: "/")
                    guard
                        let folder = await root.get(path: parts.dropLast()),
                        let file = await folder.files[String(parts.last!)]
                    else {
                        write(.error("no such file"))
                        return
                    }
                    
                    guard let content = await file.get(revision: revision) else {
                        write(.error("no such revision"))
                        return
                    }
                    
                    write(.ok("\(content.count)"))
                    write(.string(content))
                    write(.ready)
                }
                
            case .list(let dir):
                Task {
                    let parts = dir.dropFirst().split(separator: "/")
                    guard let folder = await root.get(path: parts) else {
                        write(.error("no such dir"))
                        return
                    }
                    
                    let list = await folder.list()
                    
                    write(.ok("\(list.count)"))
                    for item in list {
                        switch item {
                        case .folder(let name): write(.string("\(name)/ DIR\n"))
                        case .file(let name, let rev): write(.string("\(name) r\(rev)\n"))
                        }
                    }
                    write(.ready)
                }
                
            case .invalid(let message):
                write(.error(message))
            case .illegalMethod(let method):
                write(.error("illegal method: \(method)"))
                context.close(promise: nil)
            }
        }
    }
}

fileprivate extension UnicodeScalar {
    var isValidInPath: Bool {
        "a" <= self && self <= "z"
        || "A" <= self && self <= "Z"
        || "0" <= self && self <= "9"
        || [".", "_", "-", "/"].contains(self)
    }
}

fileprivate extension Substring {
    var validDirName: Bool { first == "/" && unicodeScalars.allSatisfy(\.isValidInPath) && !contains("//") }
    var validFileName: Bool { validDirName && last != "/" }
    var canonicalDirName: Substring { last == "/" ? dropLast() : self }
    var revision: Int? { first == "r" ? Int(dropFirst()) : Int(self) }
}

fileprivate extension UInt8 {
    var isTextChar: Bool {
        (32 <= self && self <= 127) || [9, 10].contains(self)
    }
}
