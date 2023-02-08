// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftProtohackers",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-collections.git", branch: "main"),
    ],
    targets: [
        .executableTarget(
            name: "SwiftProtohackers",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "Collections", package: "swift-collections"),
            ]),
        .testTarget(
            name: "SwiftProtohackersTests",
            dependencies: ["SwiftProtohackers"]),
    ]
)
