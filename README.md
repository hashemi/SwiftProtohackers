# SwiftProtohackers

This repository contains the solutions to the [Protohackers](https://protohackers.com/) challenges. The solutions are written in Swift using Swift NIO as the only dependency.

It currently contains solutions to the following problems:

0. Smoke Test
1. Prime Time
2. Means to an End
3. Budget Chat
4. Unusual Database Program
5. Mob in the Middle
6. Speed Daemon
7. Line Reversal

Each solution is in it's own Swift file like `Sources/SwiftProtohackers/00-SmokeTest.swift` for the first problem.

To start the server, run the following command in main directory:

```
swift run
```

This will bind to `0.0.0.0` at port `9999` and run the server serving the solution to the most recent problem solved (right now, that would be "7. Line Reversal"). To serve older solutions you'd have to checkout an earlier commit. The repo is currently organized with 1 flattened commit per problem solved.
