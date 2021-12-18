import Combine
import Foundation

extension Publisher {
    /// Await the termination of the publisher.
    ///
    /// - returns: A `Bool` indicated whether the publisher completed.
    internal func await(timeout: TimeInterval = 0.1) -> Bool {
        var done = false
        var completed = false

        let started = Date()
		_ = sink { completion in
			switch completion {
			case .finished:
				completed = true
				done = true
			case .failure:
				done = true
			}
			completed = true
		} receiveValue: { _ in
			return
		}

        while !done, abs(started.timeIntervalSinceNow) < timeout {
            RunLoop.main.run(mode: RunLoop.Mode.default, before: Date(timeIntervalSinceNow: 0.01))
        }

        return completed
    }

    /// Await the first value from the publisher.
    internal func awaitFirst() -> Result<Output, Error>? {
        var result: Result<Output, Error>?

        _ = prefix(1)
			.map(Result<Output, Error>.success)
            .catch { error -> AnyPublisher<Result<Output, Error>, Never> in
                let result = Result<Output, Error>.failure(error)
				return Just<Result<Output, Error>>(result).eraseToAnyPublisher()
            }
			.handleEvents(receiveOutput: { result = $0} )
            .await()

        return result
    }
}
