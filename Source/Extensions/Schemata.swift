import Schemata

extension AnyValue.Encoded {
	var sql: SQL.DataType {
		switch self {
		case .double, .date:
			return .real
		case .int, .unit:
			return .integer
		case .string:
			return .text
		}
	}
}

extension AnyProperty {
	var sql: SQL.Schema.Column? {
		let dataType: SQL.DataType?
		let nullable: Bool
		switch type {
		case .toMany:
			dataType = nil
			nullable = false

		case let .toOne(_, null):
			dataType = .text
			nullable = null

		case let .value(type, null):
			dataType = type.anyValue.encoded.sql
			nullable = null
		}

		return dataType.map { dataType in
			SQL.Schema.Column(
				name: path,
				type: dataType,
				nullable: nullable,
				primaryKey: path == "id"
			)
		}
	}
}

extension AnyModelValue {
	static func decode(_ value: SQL.Value) -> Any? {
		let primitive = value.primitive(anyValue.encoded)
		switch anyValue.decode(primitive) {
		case let .success(value):
			return value
		case .failure:
			return nil
		}
	}
}

extension AnySchema {
	var sql: SQL.Schema {
		return SQL.Schema(
			table: SQL.Table(name),
			columns: Set(properties.values.compactMap { $0.sql })
		)
	}
}

extension Primitive {
	var sql: SQL.Value {
		switch self {
		case let .date(date):
			return .real(date.timeIntervalSinceReferenceDate)
		case let .double(double):
			return .real(double)
		case let .int(int):
			return .integer(int)
		case .null:
			return .null
		case let .string(string):
			return .text(string)
		}
	}
}

extension Projection {
	func makeValue(_ values: [PartialKeyPath<Model>: SQL.Value]) -> Value? {
		let schema = Model.schema
		var result: [PartialKeyPath<Model>: Any] = [:]
		for (keyPath, value) in values {
			let property = schema.properties(for: keyPath).last!
			guard case let .value(type, isOptional) = property.type else {
				fatalError("keypath should end with a scalar value")
			}
			let decoded = type.decode(value)
			if isOptional {
				result[keyPath] = .some(decoded as Any)
			} else if "\(Swift.type(of: property.model))".contains("Array") {
				result[keyPath] = [decoded!]
			} else {
				result[keyPath] = decoded!
			}
		}
		return makeValue(result)
	}
}
