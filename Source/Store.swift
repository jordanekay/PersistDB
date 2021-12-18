import Combine
import CombineExt
import CombineSchedulers
import Foundation
import Schemata

/// An error that occurred while opening an on-disk `Store`.
public enum OpenError: Error {
    /// The schema of the on-disk database is incompatible with the schema of the store.
    case incompatibleSchema
    /// An unknown error occurred. An unfortunate reality.
    case unknown(Error)
}

private struct Tagged<Value> {
    let uuid: UUID
    let value: Value

    init(_ value: Value) {
        uuid = UUID()
        self.value = value
    }

    private init(uuid: UUID, value: Value) {
        self.uuid = uuid
        self.value = value
    }

    func map<NewValue>(_ transform: (Value) -> NewValue) -> Tagged<NewValue> {
        return Tagged<NewValue>(uuid: uuid, value: transform(value))
    }
}

public enum ReadOnly {}
public enum ReadWrite {}

/// A store of model objects, either in memory or on disk, that can be modified, queried, and
/// observed.
public final class Store<Mode> {
	fileprivate typealias QueueScheduler = AnyScheduler<DispatchQueue.SchedulerTimeType, DispatchQueue.SchedulerOptions>

    /// Create a new scheduler to use for database access.
	fileprivate static func makeScheduler() -> QueueScheduler {
		return DispatchQueue(label: "org.persistx.PersistDB", qos: .userInitiated).eraseToAnyScheduler()
    }

    /// The underlying SQL database.
    private let db: SQL.Database

    /// The scheduler used when accessing the database.
    private let scheduler: QueueScheduler

    /// A pipe of the actions and effects that are mutating the store.
    ///
    /// Used to determine when observed queries must be refetched.
    private let actions: PassthroughSubject<Tagged<SQL.Action>?, Never>
    private let effects: AnyPublisher<Tagged<SQL.Effect>?, Never>

    /// The designated initializer.
    ///
    /// - parameters:
    ///   - db: An opened SQL database that backs the store.
    ///   - schemas: The schemas of the models in the store.
    ///   - scheduler: The scheduler to use when accessing the database.
    ///
    /// - throws: An `OpenError` if the store cannot be created from the given database.
    ///
    /// As part of initialization, the store will verify the schema of and create tables in the
    /// database.
    private init(
        _ db: SQL.Database,
        for schemas: [AnySchema],
        scheduler: QueueScheduler = Store.makeScheduler()
    ) throws {
        self.db = db
        self.scheduler = scheduler

        let existing = Dictionary(
            uniqueKeysWithValues: db
                .schema()
                .map { ($0.table, $0) }
        )
        for schema in schemas {
            let sql = schema.sql
            if let existing = existing[sql.table] {
                if existing != sql {
                    throw OpenError.incompatibleSchema
                }
            } else if Mode.self == ReadOnly.self {
                throw OpenError.incompatibleSchema
            } else {
                db.create(sql)
            }
        }

        actions = PassthroughSubject<Tagged<SQL.Action>?, Never>()
		effects = actions
			.subscribe(on: scheduler)
            .map { action in
                return action?.map(db.perform)
            }
            .receive(on: RunLoop.main)
			.eraseToAnyPublisher()
    }

    fileprivate static func _open(
        at url: URL,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        let scheduler = Store.makeScheduler()
        return Just(url)
            .subscribe(on: scheduler)
            .tryMap { url in
				let db = try SQL.Database(at: url)
				return try Store(db, for: schemas, scheduler: scheduler)
			}
			.mapError {
				$0 as? OpenError ?? .unknown($0)
            }
            .receive(on: DispatchQueue.main)
			.eraseToAnyPublisher()
    }

    fileprivate static func _open(
        libraryNamed fileName: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return Just(fileName)
            .tryMap { fileName in
                try FileManager
                    .default
                    .url(
                        for: .applicationSupportDirectory,
                        in: .userDomainMask,
                        appropriateFor: nil,
                        create: true
                    )
                    .appendingPathComponent(fileName)
            }
            .mapError(OpenError.unknown)
            .flatMapLatest { url in
                self._open(at: url, for: schemas)
			}
			.eraseToAnyPublisher()
    }

    fileprivate static func _open(
        libraryNamed fileName: String,
        inApplicationGroup applicationGroup: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        let url = FileManager
            .default
            .containerURL(forSecurityApplicationGroupIdentifier: applicationGroup)!
            .appendingPathComponent(fileName)
        return _open(at: url, for: schemas)
            .handleEvents(receiveOutput: { store in
                let nc = CFNotificationCenterGetDarwinNotifyCenter()
                let name = CFNotificationName("\(applicationGroup)-\(fileName)" as CFString)
                _ = store
                    .effects
                    .filter { $0 != nil }
					.handleEvents(receiveOutput: { _ in
                        CFNotificationCenterPostNotification(nc, name, nil, nil, true)
                    })

                let observer = UnsafeRawPointer(Unmanaged.passUnretained(store.actions).toOpaque())
                CFNotificationCenterAddObserver(
                    nc,
                    observer,
                    { _, observer, _, _, _ in // swiftlint:disable:this opening_brace
                        if let observer = observer {
                            let actions = Unmanaged<PassthroughSubject<Tagged<SQL.Action>?, Never>>
                                .fromOpaque(observer)
                                .takeUnretainedValue()
                            actions.send(nil)
                        }
                    },
                    name.rawValue,
                    nil,
                    .deliverImmediately
                )
            })
			.eraseToAnyPublisher()
    }
}

extension Store where Mode == ReadOnly {
    /// Open an on-disk store.
    ///
    /// - parameters:
    ///   - url: The file URL of the store to open.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        at url: URL,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(at: url, for: schemas)
    }

    /// Open an on-disk store.
    ///
    /// - parameters:
    ///   - url: The file URL of the store to open.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        at url: URL,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(at: url, for: types.map { $0.anySchema })
    }

    /// Open an on-disk store inside the Application Support directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        libraryNamed fileName: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(libraryNamed: fileName, for: schemas)
    }

    /// Open an on-disk store inside the Application Support directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        libraryNamed fileName: String,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(libraryNamed: fileName, for: types.map { $0.anySchema })
    }

    /// Open an on-disk store inside the application group directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - applicationGroup: The identifier for the shared application group.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        libraryNamed fileName: String,
        inApplicationGroup applicationGroup: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(
            libraryNamed: fileName,
            inApplicationGroup: applicationGroup,
            for: schemas
        )
    }

    /// Open an on-disk store inside the application group directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - applicationGroup: The identifier for the shared application group.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public static func open(
        libraryNamed fileName: String,
        inApplicationGroup applicationGroup: String,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(
            libraryNamed: fileName,
            inApplicationGroup: applicationGroup,
            for: types.map { $0.anySchema }
        )
    }
}

extension Store where Mode == ReadWrite {
    /// Create an in-memory store for the given schemas.
    public convenience init(for schemas: [AnySchema]) {
        try! self.init(SQL.Database(), for: schemas) // swiftlint:disable:this force_try
    }

    /// Create an in-memory store for the given model types.
    public convenience init(for types: [Schemata.AnyModel.Type]) {
        self.init(for: types.map { $0.anySchema })
    }

    /// Open an on-disk store.
    ///
    /// - parameters:
    ///   - url: The file URL of the store to open.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        at url: URL,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(at: url, for: schemas)
    }

    /// Open an on-disk store.
    ///
    /// - parameters:
    ///   - url: The file URL of the store to open.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        at url: URL,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(at: url, for: types.map { $0.anySchema })
    }

    /// Open an on-disk store inside the Application Support directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        libraryNamed fileName: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(libraryNamed: fileName, for: schemas)
    }

    /// Open an on-disk store inside the Application Support directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        libraryNamed fileName: String,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(libraryNamed: fileName, for: types.map { $0.anySchema })
    }

    /// Open an on-disk store inside the application group directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - applicationGroup: The identifier for the shared application group.
    ///   - schemas: The schemas for the models in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        libraryNamed fileName: String,
        inApplicationGroup applicationGroup: String,
        for schemas: [AnySchema]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(
            libraryNamed: fileName,
            inApplicationGroup: applicationGroup,
            for: schemas
        )
    }

    /// Open an on-disk store inside the application group directory.
    ///
    /// - parameters:
    ///   - fileName: The name of the file within the Application Support directory to use for the
    ///               store.
    ///   - applicationGroup: The identifier for the shared application group.
    ///   - types: The model types in the store.
    ///
    /// - returns: A `Publisher` that will create and send a `Store` or send an `OpenError` if
    ///            one couldn't be opened.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    ///
    /// This will create a store at that URL if one doesn't already exist.
    public static func open(
        libraryNamed fileName: String,
        inApplicationGroup applicationGroup: String,
        for types: [Schemata.AnyModel.Type]
    ) -> AnyPublisher<Store, OpenError> {
        return _open(
            libraryNamed: fileName,
            inApplicationGroup: applicationGroup,
            for: types.map { $0.anySchema }
        )
    }
}

extension Store where Mode == ReadWrite {
    /// Perform an action.
    ///
    /// - parameter:
    ///   - action: The SQL action to perform.
    /// - returns: A publisher that sends the effect of the action and then completes.
    private func perform(_ action: SQL.Action) -> AnyPublisher<SQL.Effect, Never> {
        let tagged = Tagged(action)
        defer { actions.send(tagged) }

        return effects
            .compactMap { $0 }
            .filter { $0.uuid == tagged.uuid }
            .map { $0.value }
            .prefix(1)
            .share(replay: 1)
			.eraseToAnyPublisher()
    }

    /// Perform an action in the store.
    ///
    /// - important: This is done asynchronously.
    @discardableResult
    public func perform(_ action: Action) -> AnyPublisher<Never, Never> {
		let empty = Empty<SQL.Effect, Never>()
		return perform(action.makeSQL())
			.append(empty)
			.ignoreOutput()
			.eraseToAnyPublisher()
    }

    /// Insert a model entity into the store.
    ///
    /// - important: This is done asynchronously.
    ///
    /// - parameters:
    ///   - insert: The entity to insert
    /// - returns: A publisher that sends the ID after the model has been inserted.
    @discardableResult
    public func insert<Model>(_ insert: Insert<Model>) -> AnyPublisher<Model.ID, Never> {
        return perform(.insert(insert.makeSQL()))
            .map { effect -> Model.ID in
                guard case let .inserted(_, id) = effect else { fatalError("Mistaken effect") }
                let anyValue = Model.ID.anyValue
                let primitive = id.primitive(anyValue.encoded)
                switch anyValue.decode(primitive) {
                case let .success(decoded):
                    return decoded as! Model.ID // swiftlint:disable:this force_cast
                case .failure:
                    fatalError("Decoding ID should never fail")
                }
            }
			.eraseToAnyPublisher()
    }

    /// Delete a model entity from the store.
    ///
    /// - important: This is done asynchronously.
    @discardableResult
    public func delete<Model>(_ delete: Delete<Model>) -> AnyPublisher<Never, Never> {
		let empty = Empty<SQL.Effect, Never>()
        return perform(.delete(delete.makeSQL()))
			.append(empty)
			.ignoreOutput()
			.eraseToAnyPublisher()
    }

    /// Update properties for a model entity in the store.
    ///
    /// - important: This is done asynchronously.
    @discardableResult
    public func update<Model>(_ update: Update<Model>) -> AnyPublisher<Never, Never> {
		let empty = Empty<SQL.Effect, Never>()
        return perform(.update(update.makeSQL()))
			.append(empty)
			.ignoreOutput()
			.eraseToAnyPublisher()
    }
}

extension Store {
    /// Fetch a SQL query from the store.
    ///
    /// This method backs the public `fetch` and `observe` methods.
    ///
    /// - parameters:
    ///   - query: The SQL query to be fetched from the store.
    ///   - transform: A black to transform the SQL rows into a value.
    ///
    /// - returns: A `Publisher` that will fetch values for entities that match the query.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    private func fetch<Value>(
        _ query: SQL.Query,
        _ transform: @escaping ([SQL.Row]) -> Value
    ) -> AnyPublisher<Value, Never> {
        return Just(query)
            .subscribe(on: scheduler)
            .map(db.query)
            .map(transform)
            .receive(on: RunLoop.main)
			.eraseToAnyPublisher()
    }

    /// Observe a SQL query from the store.
    ///
    /// When `insert`, `delete`, or `update` is called that *might* affect the result, the
    /// value will re-fetched and re-sent.
    ///
    /// - parameters:
    ///   - query: The SQL query to be observed.
    ///   - transform: A black to transform the SQL rows into a value.
    /// - returns: A `Publisher` that will send values for entities that match the
    ////           query, sending a new value whenever it's changed.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    private func observe<Value>(
        _ query: SQL.Query,
        _ transform: @escaping ([SQL.Row]) -> Value
    ) -> AnyPublisher<Value, Never> {
		let never = Empty<Value, Never>()
        return fetch(query, transform)
            .append(never)
//			.prefix(
//				untilOutputFrom: effects
//					.filter { $0?.map(query.invalidated(by:)).value ?? true }
//					.map { _ in () }
//			)
//			.retry(.max)
			.eraseToAnyPublisher()
    }

    /// Fetch a projected query from the store.
    ///
    /// This method backs the public `fetch` and `observe` methods.
    ///
    /// - parameters:
    ///   - projected: The projected query to be fetched from the store.
    ///
    /// - returns: A `Publisher` that will fetch projections for entities that match the query.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    private func fetch<Group, Projection>(
        _ projected: ProjectedQuery<Group, Projection>
    ) -> AnyPublisher<ResultSet<Group, Projection>, Never> {
        return fetch(projected.sql, projected.resultSet(for:))
    }

    /// Observe a projected query from the store.
    ///
    /// When `insert`, `delete`, or `update` is called that *might* affect the result, the
    /// projections will be re-fetched and re-sent.
    ///
    /// - parameters:
    ///   - query: The projected query to be observed.
    ///
    /// - returns: A `Publisher` that will send sets of projections for entities that match the
    ////           query, sending a new set whenever it's changed.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    private func observe<Group, Projection>(
        _ projected: ProjectedQuery<Group, Projection>
    ) -> AnyPublisher<ResultSet<Group, Projection>, Never> {
        return observe(projected.sql, projected.resultSet(for:))
    }

    /// Fetch a projection from the store by the model entity's id.
    ///
    /// - parameters:
    ///   - id: The ID of the entity to be projected.
    ///
    /// - returns: A `Publisher` that will fetch the projection for the entity that matches the
    ///            query or send `nil` if no entity exists with that ID.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func fetch<Projection: ModelProjection>(
        _ id: Projection.Model.ID
    ) -> AnyPublisher<Projection?, Never> {
        let query = Projection.Model.all
            .filter(Projection.Model.idKeyPath == id)
        return fetch(query)
            .map { resultSet in resultSet.values.first }
			.eraseToAnyPublisher()
    }

    /// Observe a projection from the store by the model entity's id.
    ///
    /// When `insert`, `delete`, or `update` is called that *might* affect the result, the
    /// projections will be re-fetched and re-sent.
    ///
    /// - parameters:
    ///   - query: A query matching the model entities to be projected.
    ///
    /// - returns: A `Publisher` that will send sets of projections for entities that match the
    ////           query, sending a new set whenever it's changed.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func observe<Projection: ModelProjection>(
        _ id: Projection.Model.ID
    ) -> AnyPublisher<Projection?, Never> {
        let query = Projection.Model.all
            .filter(Projection.Model.idKeyPath == id)
        return observe(query)
            .map { resultSet in resultSet.first }
			.eraseToAnyPublisher()
    }

    /// Fetch projections from the store with a query.
    ///
    /// - parameters:
    ///   - query: A query matching the model entities to be projected.
    ///
    /// - returns: A `Publisher` that will fetch projections for entities that match the query.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func fetch<Key, Projection: ModelProjection>(
        _ query: Query<Key, Projection.Model>
    ) -> AnyPublisher<ResultSet<Key, Projection>, Never> {
        let projected = ProjectedQuery<Key, Projection>(query)
        return fetch(projected)
    }

    /// Observe projections from the store with a query.
    ///
    /// When `insert`, `delete`, or `update` is called that *might* affect the result, the
    /// projections will be re-fetched and re-sent.
    ///
    /// - parameters:
    ///   - query: A query matching the model entities to be projected.
    ///
    /// - returns: A `Publisher` that will send sets of projections for entities that match the
    ////           query, sending a new set whenever it's changed.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func observe<Key, Projection: ModelProjection>(
        _ query: Query<Key, Projection.Model>
    ) -> AnyPublisher<ResultSet<Key, Projection>, Never> {
        let projected = ProjectedQuery<Key, Projection>(query)
        return observe(projected)
    }

    /// Fetch an aggregate value from the store.
    ///
    /// - parameters:
    ///   - aggregate: The aggregate value to fetch.
    ///
    /// - returns: A `Publisher` that will fetch the aggregate.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func fetch<Model, Value>(
        _ aggregate: Aggregate<Model, Value>
    ) -> AnyPublisher<Value, Never> {
        return fetch(aggregate.sql, aggregate.result(for:))
    }

    /// Observe an aggregate value from the store.
    ///
    /// When `insert`, `delete`, or `update` is called that *might* affect the result, the
    /// value will be re-fetched and re-sent.
    ///
    /// - parameters:
    ///   - aggregate: The aggregate value to fetch.
    ///
    /// - returns: A `Publisher` that will send the aggregate value, sending a new value
    ///            whenever it's changed.
    ///
    /// - important: Nothing will be done until the returned producer is started.
    public func observe<Model, Value>(
        _ aggregate: Aggregate<Model, Value>
    ) -> AnyPublisher<Value, Never> {
        return observe(aggregate.sql, aggregate.result(for:))
    }
}
