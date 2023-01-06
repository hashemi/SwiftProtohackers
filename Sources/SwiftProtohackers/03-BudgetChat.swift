//
//  03-BudgetChat.swift
//  
//
//  Created by Ahmad Alhashemi on 06/01/2023.
//

import NIOCore
import NIOPosix

actor BudgetChatRoom {
    struct Client {
        let channel: Channel
        var name: String? = nil
    }

    private var clients: [ObjectIdentifier: Client] = [:]
    
    func add(channel: Channel) {
        clients[ObjectIdentifier(channel)] = Client(channel: channel)
        send(to: channel, message: "Welcome to budgetchat! What shall I call you?")
    }
    
    func remove(channel: Channel) {
        if let name = clients.removeValue(forKey: ObjectIdentifier(channel))?.name {
            broadcast(from: channel, message: "* \(name) has left the room")
        }
    }
    
    func receive(from channel: Channel, message: String) {
        let id = ObjectIdentifier(channel)
        if !clients.keys.contains(id) { return }
        
        if let name = clients[id]!.name {
            broadcast(from: channel, message: "[\(name)] \(message)")
        } else {
            let newName = message
            guard !newName.isEmpty && newName.unicodeScalars.allSatisfy({
                "0" <= $0 && $0 <= "9"
                || "a" <= $0 && $0 <= "z"
                || "A" <= $0 && $0 <= "Z"
            }) else {
                clients.removeValue(forKey: id)
                channel.close(mode: .all, promise: nil)
                return
            }
            
            let clientList = clients.values.compactMap(\.name).joined(separator: ", ")
            send(to: channel, message: "* The room contains \(clientList)")
            broadcast(from: channel, message: "* \(newName) has entered the room")
            clients[id]!.name = newName
        }
    }
    
    func send(to channel: Channel, message: String) {
        let buffer = channel.allocator.buffer(string: message + "\n")
        channel.writeAndFlush(buffer, promise: nil)
    }
    
    func broadcast(from channel: Channel, message: String) {
        let buffer = channel.allocator.buffer(string: message + "\n")
        clients
            .filter { $0.value.name != nil && $0.key != ObjectIdentifier(channel) }
            .map { $0.value.channel }
            .forEach { $0.writeAndFlush(buffer, promise: nil) }
    }
}

final class BudgetChatHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer
    
    var room = BudgetChatRoom()
    
    func channelActive(context: ChannelHandlerContext) {
        let channel = context.channel
        print("channel active")
        Task {
            await self.room.add(channel: channel)
        }
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        let channel = context.channel
        print("channel inactive")
        Task {
            await self.room.remove(channel: channel)
        }
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let channel = context.channel
        var read = self.unwrapInboundIn(data)
        let message = (read.readString(length: read.readableBytes) ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        print("channel read \(message.debugDescription)")
        Task {
            await self.room.receive(from: channel, message: message)
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: \(error)")
        context.close(promise: nil)
    }
}
