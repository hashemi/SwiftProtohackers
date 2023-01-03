import NIOCore
import NIOPosix

@main
public struct SwiftProtohackers {
    public static func main() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.addHandlers(BackPressureHandler()).flatMap { v in
                    channel.pipeline.addHandlers(SmokeTestHandler())
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 16)
            .childChannelOption(ChannelOptions.recvAllocator, value: AdaptiveRecvByteBufferAllocator())

        defer {
            try! group.syncShutdownGracefully()
        }
        
        let channel = try bootstrap.bind(host: "0.0.0.0", port: 9999).wait()
        
        print("Server started and listening on \(channel.localAddress!)")
        
        try channel.closeFuture.wait()
        
        print("Server closed")
    }
}
