// swift-tools-version:5.1

import PackageDescription

let package = Package(
    name: "PersistDB",
	platforms: [
		.iOS(.v13),
		.macOS(.v10_15),
		.watchOS(.v6),
		.tvOS(.v13)
	],
    products: [
        .library(
            name: "PersistDB",
            targets: ["PersistDB"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/PersistX/Schemata.git", from: "0.3.3"),
        .package(url: "https://github.com/tonyarnold/Differ.git", from: "1.4.3"),
		.package(url: "https://github.com/pointfreeco/combine-schedulers.git", from: "0.5.3"),
		.package(url: "https://github.com/CombineCommunity/CombineExt.git", from: "1.0.0")
    ],
    targets: [
        .target(
            name: "PersistDB",
            dependencies: [
                "Differ",
                "Schemata",
				"CombineExt",
				"CombineSchedulers"
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
