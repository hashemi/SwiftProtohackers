import NIOCore
import NIOPosix

@main
public struct SwiftProtohackers {
    public static func main() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let bootstrap = DatagramBootstrap(group: group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                channel.pipeline.addHandler(LineReversal.EncoderDecoder()).flatMap {
                    channel.pipeline.addHandler(LineReversal.LRCPHandler())
                }
            }

        defer {
            try! group.syncShutdownGracefully()
        }
        
        let channel = try bootstrap.bind(host: "0.0.0.0", port: 9999).wait()
        
        print("Server started and listening on \(channel.localAddress!)")
        
        try channel.closeFuture.wait()
        
        print("Server closed")
    }
}
