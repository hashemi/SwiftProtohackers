//
//  06-SpeedDaemon.swift
//  
//
//  Created by Ahmad Alhashemi on 07/01/2023.
//

import NIOCore

enum SpeedDaemonRequest {
    // b2 + 6
    static let plateCode: UInt8 = 0x20
    case plate(
        plate: String, // 1 + b2
        timestamp: UInt32 // 4
    )
    
    // 5
    static let wantHeartbeatCode: UInt8 = 0x40
    case wantHeartbeat(
        interval: UInt32 // 4
    )
    
    // 7
    static let iAmCameraCode: UInt8 = 0x80
    case iAmCamera(
        road: UInt16, // 2
        mile: UInt16, // 2
        limit: UInt16 // 2
    )
    
    // b2 * 2 + 2
    static let iAmDispatcherCode: UInt8 = 0x81
    case iAmDispatcher(
        numroads: UInt8, // 1
        roads: [UInt16] // b2
    )
    
    case unknown

    static let errorCode: UInt8 = 0x10 // b2 + 2
    static let ticketCode: UInt8 = 0x21 // b2 + 18

    static func waitForNextByte(_ byte: UInt8) -> Bool {
        switch byte {
        case plateCode, wantHeartbeatCode, iAmCameraCode, iAmDispatcherCode, errorCode, ticketCode:
                return true
        default:
            return false
        }
    }
    
    static func bytesRequired(_ bytes: [UInt8]) -> Int {
        switch bytes[0] {
        case plateCode:
            return Int(bytes[1] + 6)
        case wantHeartbeatCode:
            return 5
        case iAmCameraCode:
            return 7
        case iAmDispatcherCode:
            return ((Int(bytes[1]) * 2) + 2)
        case errorCode:
            return Int(bytes[1]) + 2
        case ticketCode:
            return Int(bytes[1]) + 18
        case _:
            return 1
        }
    }
    
    init(buffer: inout ByteBuffer) {
        switch buffer.readInteger(as: UInt8.self) {
        case Self.plateCode:
            let plateLength = Int(buffer.readInteger(as: UInt8.self)!)
            let plateBytes = buffer.readBytes(length: plateLength)!
            let plate = String(bytes: plateBytes, encoding: .utf8)!
            let timestamp = buffer.readInteger(as: UInt32.self)!
            self = .plate(plate: plate, timestamp: timestamp)

        case Self.wantHeartbeatCode:
            let interval = buffer.readInteger(as: UInt32.self)!
            self = .wantHeartbeat(interval: interval)
            
        case Self.iAmCameraCode:
            let road = buffer.readInteger(as: UInt16.self)!
            let mile = buffer.readInteger(as: UInt16.self)!
            let limit = buffer.readInteger(as: UInt16.self)!
            self = .iAmCamera(road: road, mile: mile, limit: limit)
            
        case Self.iAmDispatcherCode:
            let numroads = buffer.readInteger(as: UInt8.self)!
            let roads: [UInt16] = (0..<numroads).map { _ in
                buffer.readInteger(as: UInt16.self)!
            }
            self = .iAmDispatcher(numroads: numroads, roads: roads)
        case _:
            self = .unknown
        }
    }
}

enum SpeedDaemonResponse {
    static let errorCode: UInt8 = 0x10
    case error(
        msg: String
    )
    
    static let ticketCode: UInt8 = 0x21
    case ticket(
        plate: String,
        road: UInt16,
        mile1: UInt16,
        timestamp1: UInt32,
        mile2: UInt16,
        timestamp2: UInt32,
        speed: UInt16
    )
    
    static let heartbeatCode: UInt8 = 0x41
    case heartbeat
    
    func write(to buffer: inout ByteBuffer) {
        switch self {
        case let .error(msg):
            buffer.writeInteger(Self.errorCode)
            buffer.writeInteger(UInt8(msg.utf8.count))
            buffer.writeString(msg)
        case let .ticket(plate, road, mile1, timestamp1, mile2, timestamp2, speed):
            buffer.writeInteger(Self.ticketCode)
            buffer.writeInteger(UInt8(plate.utf8.count))
            buffer.writeString(plate)
            buffer.writeInteger(road)
            buffer.writeInteger(mile1)
            buffer.writeInteger(timestamp1)
            buffer.writeInteger(mile2)
            buffer.writeInteger(timestamp2)
            buffer.writeInteger(speed)
        case .heartbeat:
            buffer.writeInteger(Self.heartbeatCode)
        }
    }
}

final class SpeedDaemonRequestDecoder: ByteToMessageDecoder {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = SpeedDaemonRequest
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        if buffer.readableBytes >= 1 {
            let byte = buffer.getBytes(at: buffer.readerIndex, length: 1)![0]
            if !SpeedDaemonRequest.waitForNextByte(byte) {
                buffer.moveReaderIndex(forwardBy: 1)
                context.fireChannelRead(wrapInboundOut(.unknown))
                return .continue
            }
        }
        
        if buffer.readableBytes < 2 {
            return .needMoreData
        }
        
        let bytesRequired = SpeedDaemonRequest.bytesRequired(buffer.getBytes(at: buffer.readerIndex, length: 2)!)
        
        if buffer.readableBytes < bytesRequired {
            return .needMoreData
        }
        
        var slice = buffer.readSlice(length: bytesRequired)!
        let message = SpeedDaemonRequest(buffer: &slice)
        context.fireChannelRead(wrapInboundOut(message))

        return .continue
    }
}

final class SpeedDaemonResponseEncoder: MessageToByteEncoder {
    typealias OutboundIn = SpeedDaemonResponse
    typealias OutboundOut = ByteBuffer

    func encode(data: SpeedDaemonResponse, out: inout ByteBuffer) throws {
        if case .heartbeat = data {
        } else {
            print("--> \(data)")
        }

        data.write(to: &out)
    }
}

struct SpeedDaemonPlate: Hashable {
    let plate: String
}

struct SpeedDaemonTicket {
    let plate: SpeedDaemonPlate
    let road: Int
    let mile1: Int
    let timestamp1: Int
    let mile2: Int
    let timestamp2: Int
    let speed: Int
    
    var response: SpeedDaemonResponse {
        .ticket(
            plate: plate.plate,
            road: UInt16(road),
            mile1: UInt16(mile1),
            timestamp1: UInt32(timestamp1),
            mile2: UInt16(mile2),
            timestamp2: UInt32(timestamp2),
            speed: UInt16(speed)
        )
    }
}

struct SpeedDaemonCameraObservation {
    let plate: SpeedDaemonPlate
    let mile: Int
    let timestamp: Int
}

actor SpeedDaemonRoad {
    let number: Int
    let limit: Int
    let dispatchCenter: SpeedDaemonDispatchCenter
    var observations: [SpeedDaemonPlate: [SpeedDaemonCameraObservation]] = [:]
    
    init(number: Int, limit: Int, dispatchCenter: SpeedDaemonDispatchCenter) {
        self.number = number
        self.limit = limit
        self.dispatchCenter = dispatchCenter
    }
    
    func report(observation obs: SpeedDaemonCameraObservation) {
        defer { observations[obs.plate, default: []].append(obs) }
        
        for prevObs in observations[obs.plate, default: []] {
            let sorted = [obs, prevObs].sorted { $0.timestamp < $1.timestamp }
            let distance = abs(Double(sorted[1].mile) - Double(sorted[0].mile))
            let time = (Double(sorted[1].timestamp) - Double(sorted[0].timestamp)) / (60.0 * 60.0)
            guard distance > 0 && time > 0 else { continue }
            let speed = Int((distance / time) * 100)
            let overlimit = speed > (self.limit * 100)

            if overlimit {
                Task {
                    await dispatchCenter.report(ticket: SpeedDaemonTicket(
                        plate: obs.plate,
                        road: self.number,
                        mile1: sorted[0].mile,
                        timestamp1: sorted[0].timestamp,
                        mile2: sorted[1].mile,
                        timestamp2: sorted[1].timestamp,
                        speed: speed
                    ))
                }
            }
        }
    }
}

actor SpeedDaemonDispatchCenter {
    var roads: [Int: SpeedDaemonRoad] = [:]
    var dispatchers: [Int: [Channel]] = [:]
    var ticketedDays: [SpeedDaemonPlate: Set<Int>] = [:]
    var pendingTickets: [Int: [SpeedDaemonTicket]] = [:]
    
    subscript(road road: Int, limit limit: Int) -> SpeedDaemonRoad {
        if !roads.keys.contains(road) {
            roads[road] = SpeedDaemonRoad(
                number: road,
                limit: limit,
                dispatchCenter: self
            )
        }
        return roads[road]!
    }
    
    func register(dispatcher: Channel, for roads: [Int]) {
        var ticketsToHandle: [SpeedDaemonTicket] = []
        for road in roads {
            dispatchers[road, default: []].append(dispatcher)
            
            if let removedTickets = pendingTickets.removeValue(forKey: road) {
                ticketsToHandle.append(contentsOf: removedTickets)
            }
        }
        for ticket in ticketsToHandle {
            dispatcher.write(ticket.response, promise: nil)
        }
        if !ticketsToHandle.isEmpty {
            dispatcher.flush()
        }
    }
    
    func report(ticket: SpeedDaemonTicket) {
        let day1 = ticket.timestamp1 / 86400
        let day2 = ticket.timestamp2 / 86400
        
        if
            let plateTicketedDays = ticketedDays[ticket.plate],
            (day1...day2).contains(where: { plateTicketedDays.contains($0) }) {
            // Drop the ticket
            return
        }
        
        ticketedDays[ticket.plate, default: []].formUnion(day1...day2)
        
        guard let dispatcher = dispatchers[ticket.road, default: []].filter(\.isActive).randomElement() else {
            // Store the ticket
            pendingTickets[ticket.road, default: []].append(ticket)
            return
        }
        
        // Send the ticket to a random dispatcher
        dispatcher.writeAndFlush(ticket.response, promise: nil)
    }
}

final class SpeedDaemonHandler: ChannelInboundHandler {
    typealias InboundIn = SpeedDaemonRequest
    typealias OutboundOut = SpeedDaemonResponse
    
    enum Status {
        case undefined
        case cameraInProcess(mile: Int)
        case camera(road: SpeedDaemonRoad, mile: Int)
        case dispatcher
    }
    
    var observationBacklog: [SpeedDaemonCameraObservation] = []
    
    private let dispatchCenter: SpeedDaemonDispatchCenter
    private var status = Status.undefined {
        didSet {
            if case let .camera(road, _) = status {
                Task {
                    for obs in observationBacklog {
                        await road.report(observation: obs)
                    }
                }
            }
        }
    }
    
    init(dispatchCenter: SpeedDaemonDispatchCenter) {
        self.dispatchCenter = dispatchCenter
    }
    
    private var hasHeartbeat: Bool = false

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let request = unwrapInboundIn(data)
        let channel = context.channel
        
        print("<-- \(request)")

        switch (request, status) {
        case let (.wantHeartbeat(interval), _) where !hasHeartbeat:
            if interval != 0 {
                context.eventLoop.scheduleRepeatedTask(initialDelay: .zero, delay: .milliseconds(Int64(interval) * 100)) { _ in
                    channel.writeAndFlush(SpeedDaemonResponse.heartbeat, promise: nil)
                }
            }
            hasHeartbeat = true
        case let (.iAmCamera(roadNumber, mile, limit), .undefined):
            self.status = .cameraInProcess(mile: Int(mile))
            Task {
                let road = await dispatchCenter[road: Int(roadNumber), limit: Int(limit)]
                channel.eventLoop.execute {
                    self.status = .camera(road: road, mile: Int(mile))
                }
            }
        case let (.iAmDispatcher(_, roads), .undefined):
            self.status = .dispatcher
            Task {
                await dispatchCenter.register(dispatcher: channel, for: roads.map(Int.init))
            }
        case let (.plate(plate, timestamp), .camera(road, mile)):
            Task {
                await road.report(observation: SpeedDaemonCameraObservation(
                    plate: SpeedDaemonPlate(plate: plate),
                    mile: mile,
                    timestamp: Int(timestamp)
                ))
            }
        case let (.plate(plate, timestamp), .cameraInProcess(mile)):
            observationBacklog.append(SpeedDaemonCameraObservation(
                plate: SpeedDaemonPlate(plate: plate),
                mile: mile,
                timestamp: Int(timestamp))
            )
        default:
            context.writeAndFlush(wrapOutboundOut(.error(msg: "Unexpected message for connection")), promise: nil)
        }
    }
}

