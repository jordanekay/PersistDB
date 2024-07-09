// swift-tools-version:5.10

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
        .package(url: "https://github.com/Fleuronic/Schemata.git", branch: "master"),
        .package(url: "https://github.com/ReactiveCocoa/ReactiveSwift.git", branch: "swift-concurrency"),
        .package(url: "https://github.com/tonyarnold/Differ.git", from: "1.4.3"),
    ],
    targets: [
        .target(
            name: "PersistDB",
            dependencies: [
                "Differ",
                "ReactiveSwift",
                "Schemata",
            ],
            path: "Source"
        ),
        .testTarget(
            name: "PersistDBTests",
            dependencies: ["PersistDB"],
            path: "Tests"
        ),
    ],
    swiftLanguageVersions: [.v5]
)
