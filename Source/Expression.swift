import Foundation
import ReactiveSwift
import Schemata

/// A type-erased expression.
///
/// This represents the expressions representable in PersistDB. As such, this is a less general, but
/// more semantically meaningful, representation than `SQL.Expression`.
///
/// `AnyExpression`s can also generate new values, e.g. UUIDs, which are expressed as values in SQL.
/// These values are expressed as values within `AnyExpression`, but the act of generating the
/// corresponding `SQL.Expression` causes it to generate any such values.
internal indirect enum AnyExpression: Hashable {
    internal enum UnaryOperator: String, Hashable {
        case not
    }

    internal enum BinaryOperator: String, Hashable {
        case and
        case equal
        case greaterThan
        case greaterThanOrEqual
        case lessThan
        case lessThanOrEqual
        case notEqual
        case or
    }

    internal enum Function: Hashable {
        case coalesce
        case count
        case length
        case max
        case min
    }

    case binary(BinaryOperator, AnyExpression, AnyExpression)
    case function(Function, [AnyExpression])
    case inList(AnyExpression, Set<AnyExpression>)
    case keyPath([AnyProperty])
    case now
    case unary(UnaryOperator, AnyExpression)
    case value(SQL.Value)
}

extension AnyExpression.UnaryOperator {
    var sql: SQL.UnaryOperator {
        switch self {
        case .not:
            return .not
        }
    }
}

extension AnyExpression.BinaryOperator {
    var sql: SQL.BinaryOperator {
        switch self {
        case .and:
            return .and
        case .equal:
            return .equal
        case .greaterThan:
            return .greaterThan
        case .greaterThanOrEqual:
            return .greaterThanOrEqual
        case .lessThan:
            return .lessThan
        case .lessThanOrEqual:
            return .lessThanOrEqual
        case .notEqual:
            return .notEqual
        case .or:
            return .or
        }
    }
}

extension AnyExpression {
    init<Model: PersistDB.Model>(_ keyPath: PartialKeyPath<Model>) {
        self = .keyPath(Model.anySchema.properties(for: keyPath))
    }

    init<V: ModelValue>(_ value: V) {
        self = .value(V.anyValue.encode(value).sql)
    }

    init<V: ModelValue>(_ value: V?) {
        self = .value(value.map(V.anyValue.encode)?.sql ?? .null)
    }
}

extension AnyExpression.Function {
    fileprivate var sql: SQL.Function {
        switch self {
        case .coalesce:
            return .coalesce
        case .count:
            return .count
        case .length:
            return .length
        case .max:
            return .max
        case .min:
            return .min
        }
    }
}

extension SQL {
    fileprivate static var now: SQL.Expression {
        let seconds = SQL.Expression.function(
            .strftime, [
                .value(.text("%s")),
                .value(.text("now")),
            ]
        )
        let subseconds = SQL.Expression.function(
            .substr, [
                .function(
                    .strftime, [
                        .value(.text("%f")),
                        .value(.text("now")),
                    ]
                ),
                .value(.integer(4)),
            ]
        )
        return .cast(
            .binary(
                .concatenate,
                .binary(
                    .subtract,
                    seconds,
                    .value(.integer(Int(Date.timeIntervalBetween1970AndReferenceDate)))
                ),
                .binary(
                    .concatenate,
                    .value(.text(".")),
                    subseconds
                )
            ),
            .real
        )
    }
}

private func makeSQL(for properties: [AnyProperty]) -> SQL.Expression {
    func column(for property: AnyProperty) -> SQL.Column {
		let anyModel = property.model as! (any AnyModel.Type)
		return SQL.Table(anyModel.anySchema.name)[property.path]
    }

    var value: SQL.Expression = .column(column(for: properties.last!))

    for property in properties.reversed().dropFirst() {
		let anyModel = property.model as! (any AnyModel.Type)
        switch property.type {
        case let .toMany(model):
			let lhs = SQL.Column(
				table: SQL.Table(anyModel.anySchema.name),
				name: "id"
			)
			let rhs = SQL.Column(
				table: SQL.Table(model.anySchema.name),
				name: property.path
			)
			value = .join(lhs, rhs, value)
		case let .toOne(model, _):
			let lhs = SQL.Table(anyModel.anySchema.name)[property.path]
            let rhs = SQL.Column(
				table: SQL.Table(model.anySchema.name),
                name: "id"
            )
            value = .join(lhs, rhs, value)
        case .value:
            fatalError("Invalid scalar property in the middle of a KeyPath")
        }
    }
    return value
}

extension AnyExpression {
    var sql: SQL.Expression {
        switch self {
        case let .binary(.equal, .value(.null), rhs):
            return .binary(.is, rhs.sql, .value(.null))
        case let .binary(.equal, lhs, .value(.null)):
            return .binary(.is, lhs.sql, .value(.null))
        case let .binary(.notEqual, .value(.null), rhs):
            return .binary(.isNot, rhs.sql, .value(.null))
        case let .binary(.notEqual, lhs, .value(.null)):
            return .binary(.isNot, lhs.sql, .value(.null))
        case let .binary(op, lhs, rhs):
            return .binary(op.sql, lhs.sql, rhs.sql)
        case let .function(function, args):
            return .function(function.sql, args.map { $0.sql })
        case let .inList(expr, list):
            return .inList(expr.sql, Set(list.map { $0.sql }))
        case let .keyPath(properties):
            return makeSQL(for: properties)
        case .now:
            return SQL.now
        case let .unary(op, expr):
            return .unary(op.sql, expr.sql)
        case let .value(value):
            return .value(value)
        }
    }
}

/// An expression that can be used in `Predicate`s, `Ordering`s, etc.
public struct Expression<Model, Value>: Hashable {
    internal let expression: AnyExpression

    internal init(_ expression: AnyExpression) {
        self.expression = expression
    }
}

extension Expression {
    public var dictionary: [String: any Sendable] {
        expression.sql.dictionary
    }
}

extension SQL.Expression {
    fileprivate var dictionary: [String: any Sendable] {
        switch self {
        case let .binary(`operator`, .column(column), .value(value)):
            return dictionary(column: column, operator: `operator`, value: value)
        case let .binary(`operator`, .join(outerColumn, _, .column(innerColumn)), .value(value)):
            return [outerColumn.name: dictionary(column: innerColumn, operator: `operator`, value: value)]
        case let .binary(`operator`, lhs, rhs):
            return [`operator`.rawValue: lhs.dictionary.merging(rhs.dictionary) { $1 }]
        default:
            return [:]
        }
    }

    private func dictionary(
        column: SQL.Column,
        operator: SQL.BinaryOperator,
        value: SQL.Value
    ) -> [String: any Sendable] {
        [column.name: [`operator`.rawValue: value.text ?? value.description]]
    }
}

extension Expression where Model: PersistDB.Model {
    /// Create an expression from a keypath.
    public init(_ keyPath: KeyPath<Model, Value>) {
        expression = AnyExpression(keyPath)
    }
}

extension Expression where Model == None, Value == Date {
    /// An expression that evaluates to the current datetime.
    public static var now: Expression {
        return Expression(AnyExpression.now)
    }
}

extension Expression where Model == None, Value: ModelValue {
    public init(_ value: Value) {
        expression = .value(Value.anyValue.encode(value).sql)
    }
}

extension Expression where Model == None, Value: OptionalProtocol, Value.Wrapped: ModelValue {
    public init(_ value: Value?) {
        expression = .value(value.map(Value.Wrapped.anyValue.encode)?.sql ?? .null)
    }
}

extension Expression where Value == String {
    /// The number of characters in the string prior to the first null character.
    public var count: Expression<Model, Int> {
        return Expression<Model, Int>(.function(.length, [ expression ]))
    }
}

// MARK: - Operators

internal func == (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.equal, lhs, rhs)
}

internal func != (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.notEqual, lhs, rhs)
}

internal func && (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.and, lhs, rhs)
}

internal func || (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.or, lhs, rhs)
}

internal prefix func ! (expression: AnyExpression) -> AnyExpression {
    return .unary(.not, expression)
}

internal func < (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.lessThan, lhs, rhs)
}

internal func > (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.greaterThan, lhs, rhs)
}

internal func <= (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.lessThanOrEqual, lhs, rhs)
}

internal func >= (lhs: AnyExpression, rhs: AnyExpression) -> AnyExpression {
    return .binary(.greaterThanOrEqual, lhs, rhs)
}

// MARK: - Aggregates

internal func max(_ expressions: [AnyExpression]) -> AnyExpression {
    return .function(.max, expressions)
}

internal func max(_ expressions: AnyExpression...) -> AnyExpression {
    return max(expressions)
}

internal func min(_ expressions: [AnyExpression]) -> AnyExpression {
    return .function(.min, expressions)
}

internal func min(_ expressions: AnyExpression...) -> AnyExpression {
    return min(expressions)
}

// MARK: - Collections

extension Collection where Iterator.Element: ModelValue {
    /// An expression that tests whether the list contains the value of an
    /// expression.
    internal func contains(_ expression: AnyExpression) -> AnyExpression {
        return .inList(expression, Set(map(AnyExpression.init)))
    }
}

// MARK: - Functions

/// Evaluates to the first non-NULL argument, or NULL if all argumnets are NULL.
public func coalesce<Model: PersistDB.Model, Value>(
    _ a: KeyPath<Model, Value?>,
    _ b: KeyPath<Model, Value?>,
    _ rest: KeyPath<Model, Value?>...
) -> Expression<Model, Value?> {
    let args = ([a, b] + rest)
        .map(Model.anySchema.properties(for:))
        .map(AnyExpression.keyPath)
    return Expression(.function(.coalesce, args))
}
