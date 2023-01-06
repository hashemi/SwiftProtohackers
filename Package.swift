// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftProtohackers",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(url: "https://github.com/hashemi/swift-nio.git", branch: "allow-empty-datagrams")
    ],
    targets: [
        .executableTarget(
            name: "SwiftProtohackers",
            dependencies: [.product(name: "NIOCore", package: "swift-nio"),
                           .product(name: "NIOPosix", package: "swift-nio"),
                           .product(name: "NIOHTTP1", package: "swift-nio"),
                           .product(name: "NIOFoundationCompat", package: "swift-nio")]),
        .testTarget(
            name: "SwiftProtohackersTests",
            dependencies: ["SwiftProtohackers"]),
    ]
)
