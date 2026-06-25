// swift-tools-version:5.8

import PackageDescription

let package = Package(
    name: "PersistDB",
    products: [
        .library(
            name: "PersistDB",
            targets: ["PersistDB"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/jordanekay/Schemata", branch: "master"),
        .package(url: "https://github.com/ReactiveCocoa/ReactiveSwift.git", from: "7.1.1"),
        .package(url: "https://github.com/tonyarnold/Differ", from: "1.4.3")
    ],
    targets: [
        .systemLibrary(
            name: "CSQLite",
            path: "CSQLite",
            pkgConfig: "sqlite3",
            providers: [
                .apt(["libsqlite3-dev"]),
                .brew(["sqlite3"])
            ]
        ),
        .target(
            name: "PersistDB",
            dependencies: [
                "Differ",
                "ReactiveSwift",
                "Schemata",
                .target(name: "CSQLite", condition: .when(platforms: [.linux])),
            ],
            path: "Source"
        )
    ],
    swiftLanguageVersions: [.v5]
)
