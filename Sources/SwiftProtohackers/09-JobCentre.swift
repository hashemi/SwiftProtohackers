//
//  09-JobCentre.swift
//  
//
//  Created by Ahmad Alhashemi on 21/01/2023.
//

import NIOCore
import Foundation
import NIOFoundationCompat
import SortedCollections

enum AnyJSON: Codable {
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)
    case object([String: AnyJSON])
    case array([AnyJSON])
    
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode(Int.self) {
            self = .int(value)
        } else if let value = try? container.decode(Double.self) {
            self = .double(value)
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode([String: AnyJSON].self) {
            self = .object(value)
        } else if let value = try? container.decode([AnyJSON].self) {
            self = .array(value)
        } else {
            throw DecodingError.typeMismatch(AnyJSON.self, DecodingError.Context(codingPath: container.codingPath, debugDescription: "Not a JSON"))
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case let .string(v): try container.encode(v)
        case let .int(v): try container.encode(v)
        case let .double(v): try container.encode(v)
        case let .bool(v): try container.encode(v)
        case let .object(v): try container.encode(v)
        case let .array(v): try container.encode(v)
        }
    }
}

enum JobCenter {
    enum Request: Decodable {
        case put(queue: String, job: AnyJSON, pri: Int)
        case get(queues: [String], wait: Bool)
        case delete(id: Int)
        case abort(id: Int)
        case error(message: String)
        
        enum CodingKeys: CodingKey {
            case request, queue, job, pri, queues, wait, id
        }
        
        init(from decoder: Swift.Decoder) throws {
            let values = try decoder.container(keyedBy: CodingKeys.self)
            let request = try values.decode(String.self, forKey: .request)
            switch request {
            case "put":
                self = .put(
                    queue: try values.decode(String.self, forKey: .queue),
                    job: try values.decode(AnyJSON.self, forKey: .job),
                    pri: try values.decode(Int.self, forKey: .pri)
                )
            case "get":
                self = .get(
                    queues: try values.decode([String].self, forKey: .queues),
                    wait: (try? values.decode(Bool.self, forKey: .wait)) ?? false
                )
            case "delete":
                self = .delete(id: try values.decode(Int.self, forKey: .id))
            case "abort":
                self = .abort(id: try values.decode(Int.self, forKey: .id))
            default:
                throw DecodingError.typeMismatch(Request.self, DecodingError.Context(codingPath: values.codingPath, debugDescription: "Unknown request type \(request)"))
            }
        }
    }
    
    enum Response: Encodable {
        case ok(id: Int)
        case job(id: Int, job: AnyJSON, pri: Int, queue: String)
        case noJob
        case error(message: String)
        
        enum CodingKeys: CodingKey {
            case status, id, job, pri, queue, error
        }
        
        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case let .ok(id: id):
                try container.encode("ok", forKey: .status)
                try container.encode(id, forKey: .id)
            case let .job(id, job, pri, queue):
                try container.encode("ok", forKey: .status)
                try container.encode(id, forKey: .id)
                try container.encode(job, forKey: .job)
                try container.encode(pri, forKey: .pri)
                try container.encode(queue, forKey: .queue)
            case .noJob:
                try container.encode("no-job", forKey: .status)
            case let .error(message):
                try container.encode("error", forKey: .status)
                try container.encode(message, forKey: .error)
            }
        }
    }
    
    final class RequestDecoder: ByteToMessageDecoder {
        typealias InboundIn = ByteBuffer
        typealias InboundOut = Request
        
        func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            guard let newlineIdx = buffer.readableBytesView.firstIndex(of: UInt8(ascii: "\n")) else {
                return .needMoreData
            }
            
            let length = newlineIdx - buffer.readerIndex + 1
            let request: Request
            do {
                request = try buffer.readJSONDecodable(Request.self, decoder: JSONDecoder(), length: length) ?? .error(message: "got nil on trying to decode json")
            } catch {
                request = .error(message: error.localizedDescription)
                buffer.moveReaderIndex(to: newlineIdx + 1)
            }
            context.fireChannelRead(wrapInboundOut(request))
            return .continue
        }
    }

    final class ResponseEncoder: MessageToByteEncoder {
        typealias OutboundIn = Response
        
        func encode(data: JobCenter.Response, out: inout ByteBuffer) throws {
            try out.writeJSONEncodable(data)
            out.writeStaticString("\n")
        }
    }
    
    final class Handler: ChannelInboundHandler {
        typealias InboundIn = Request
        typealias OutboundOut = Response
        
        var center: Center
        var continuation: AsyncStream<Request>.Continuation!
        var task: Task<Void, Error>!
        
        init(center: Center) {
            self.center = center
        }

        func channelActive(context: ChannelHandlerContext) {
            let stream = AsyncStream { self.continuation = $0 }
            let channel = context.channel
            let workerId = ObjectIdentifier(channel)
            
            task = Task {
                for await request in stream {
                    func write(_ response: Response) {
                        channel.writeAndFlush(self.wrapOutboundOut(response), promise: nil)
                    }

                    switch request {
                    case let .put(queue, job, pri):
                        let id = await center.put(queue: queue, job: job, pri: pri)
                        write(.ok(id: id))
                    case let .get(queues, wait):
                        if let job = await center.get(queues: queues, wait: wait, workerId: workerId) {
                            write(.job(id: job.id, job: job.details, pri: job.priority, queue: job.queue))
                        } else if !wait {
                            write(.noJob)
                        }
                    case let .abort(id):
                        switch await center.abort(id: id, worker: workerId) {
                        case .some(true):
                            write(.ok(id: id))
                        case .some(false):
                            write(.noJob)
                        case .none:
                            write(.error(message: "you cannot abort a job assigned to someone else"))
                        }
                    case let .delete(id: id):
                        write(await center.delete(id: id) ? .ok(id: id) : .noJob)
                    case let .error(message: message):
                        write(.error(message: message))
                    }
                }
            }
        }
        
        func channelInactive(context: ChannelHandlerContext) {
            let workerId = ObjectIdentifier(context.channel)
            Task {
                await center.disconnect(worker: workerId)
            }
        }
        
        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            self.continuation.yield(unwrapInboundIn(data))
        }
    }
    
    actor Center {
        struct Job {
            enum Status: Equatable { case pending, assigned(ObjectIdentifier), deleted }
            
            var status: Status
            let id: Int
            let queue: String
            let priority: Int
            let details: AnyJSON
        }
        
        struct JobReference: Hashable, Comparable {
            static func < (lhs: JobCenter.Center.JobReference, rhs: JobCenter.Center.JobReference) -> Bool {
                lhs.priority < rhs.priority
            }
            
            let id: Int
            let priority: Int
        }

        var jobs: [Job] = []
        var pendingJobsByQueue: [String: SortedSet<JobReference>] = [:]
        var assignedJobsByWorker: [ObjectIdentifier: Set<Int>] = [:]
        var waiting: [ObjectIdentifier: [CheckedContinuation<Job?, Never>]] = [:]
        var waitingByQueue: [String: Set<ObjectIdentifier>] = [:]
        
        func put(queue: String, job: AnyJSON, pri: Int) -> Int {
            let id = jobs.count
            
            jobs.append(Job(
                status: .pending,
                id: id,
                queue: queue,
                priority: pri,
                details: job
            ))
            
            pendingJobsByQueue[queue, default: []].insert(JobReference(id: id, priority: pri))

            checkWaiting(id: id)
            
            return id
        }
        
        private func removeWorkerFromQueues(_ worker: ObjectIdentifier) {
            for queue in waitingByQueue.keys {
                waitingByQueue[queue]!.remove(worker)
            }
        }
        
        private func checkWaiting(id: Int) {
            guard jobs[id].status == .pending else { return }
            guard let worker = waitingByQueue[jobs[id].queue, default: []].popFirst() else { return }

            jobs[id].status = .assigned(worker)
            pendingJobsByQueue[jobs[id].queue]?.remove(JobReference(id: id, priority: jobs[id].priority))
            assignedJobsByWorker[worker, default: []].insert(id)
            waiting.removeValue(forKey: worker)?.first!.resume(returning: jobs[id])
        }
        
        func get(queues: [String], wait: Bool, workerId: ObjectIdentifier) async -> Job? {
            let highestPriority = queues
                .compactMap { pendingJobsByQueue[$0]?.last }
                .max { jobs[$0.id].priority < jobs[$1.id].priority }

            if let highestPriority = highestPriority {
                jobs[highestPriority.id].status = .assigned(workerId)
                _ = pendingJobsByQueue[jobs[highestPriority.id].queue]!.removeLast()
                assignedJobsByWorker[workerId, default: []].insert(highestPriority.id)
                return jobs[highestPriority.id]
            }
            
            if wait {
                return await withCheckedContinuation {
                    waiting[workerId, default: []].append($0)
                    for queue in queues {
                        waitingByQueue[queue, default: []].insert(workerId)
                    }
                }
            }
            
            return nil
        }
        
        func abort(id: Int, worker: ObjectIdentifier) -> Bool? {
            guard
                id >= 0 && id < jobs.count,
                case let .assigned(owner) = jobs[id].status
            else { return false }

            if owner == worker {
                jobs[id].status = .pending
                pendingJobsByQueue[jobs[id].queue, default: []].insert(JobReference(id: id, priority: jobs[id].priority))
                assignedJobsByWorker[worker]?.remove(id)
                checkWaiting(id: id)
                return true
            } else {
                return nil
            }
        }
        
        func delete(id: Int) -> Bool {
            guard id >= 0 && id < jobs.count && jobs[id].status != .deleted else { return false }
            pendingJobsByQueue[jobs[id].queue]?.remove(JobReference(id: id, priority: jobs[id].priority))
            if case let .assigned(worker) = jobs[id].status {
                assignedJobsByWorker[worker]?.remove(id)
            }
            jobs[id].status = .deleted
            return true
        }
        
        func disconnect(worker: ObjectIdentifier) {
            if let continuation = waiting.removeValue(forKey: worker)?.first {
                continuation.resume(returning: nil)
            }
            removeWorkerFromQueues(worker)

            for idx in assignedJobsByWorker.removeValue(forKey: worker) ?? [] {
                _ = abort(id: idx, worker: worker)
            }
        }
    }
}
