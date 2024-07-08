@testable import PersistDB
import XCTest

class UpdateSQLTests: XCTestCase {
    func testSQLWithPredicate() {
        let predicate: PersistDB.Predicate = \Widget.id == 1
        let update = Update(
            predicate: predicate,
            valueSet: [
                \.date == Date.now,
                \.double == 4.7,
            ]
        )

        let sql = SQL.Update(
            table: SQL.Table("Widget"),
            values: [
                "date": AnyExpression.now.sql,
                "double": .value(.real(4.7)),
            ],
            predicate: predicate.expression.sql
        )
        XCTAssertEqual(update.makeSQL(), sql)
    }

    func testSQLWithoutPredicate() {
        let update = Update(
            predicate: nil,
            valueSet: [
                \Widget.date == Date.now,
                \Widget.double == 4.7,
            ]
        )

        let sql = SQL.Update(
            table: SQL.Table("Widget"),
            values: [
                "date": AnyExpression.now.sql,
                "double": .value(.real(4.7)),
            ],
            predicate: nil
        )
        XCTAssertEqual(update.makeSQL(), sql)
    }
}
