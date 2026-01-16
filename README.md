# k8sgraph

A Go library for discovering Kubernetes resources and building graph representations. Supports Neo4j and Zep backends.

## Installation

```bash
go get github.com/opsweave/k8sgraph
```

## Features

- **K8s Resource Discovery**: Automatically discovers all Kubernetes resources including CRDs
- **Relationship Extraction**: Builds relationships between resources:
  - `OWNS` - Owner references
  - `SELECTS` - Service to Pod selectors
  - `USES_CONFIGMAP` - Pod to ConfigMap references
  - `USES_SECRET` - Pod to Secret references
- **Event Facts**: Extracts K8s events as text facts for LLM ingestion
- **Multiple Backends**: Supports Neo4j and Zep graph databases

## Usage

### Basic Discovery

```go
package main

import (
    "context"
    "log"

    "github.com/opsweave/k8sgraph"
)

func main() {
    ctx := context.Background()

    cfg := k8sgraph.Config{
        AllNamespaces: true,
        Debug:         true,
    }

    // Discover K8s resources
    nodes, rels, err := k8sgraph.DiscoverGraph(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Discovered %d nodes and %d relationships", len(nodes), len(rels))
}
```

### Write to Neo4j

```go
cfg := k8sgraph.Config{
    Neo4jURI:      "neo4j://localhost:7687",
    Neo4jUser:     "neo4j",
    Neo4jPassword: "password",
    Neo4jDatabase: "neo4j",
    AllNamespaces: true,
}

// Discover and write to Neo4j
if err := k8sgraph.BuildGraph(ctx, cfg); err != nil {
    log.Fatal(err)
}
```

### Write to Zep

```go
cfg := k8sgraph.Config{
    ZepAPIKey:    "your-api-key",
    ZepGraphID:   "k8s-graph",
    ZepGraphName: "Kubernetes Graph",
    AllNamespaces: true,
}

nodes, rels, err := k8sgraph.DiscoverGraph(ctx, cfg)
if err != nil {
    log.Fatal(err)
}

// Write to Zep (single payload)
if err := k8sgraph.WriteZepSingle(ctx, &cfg, nodes, rels); err != nil {
    // Fallback to chunked if too large
    if err := k8sgraph.WriteZep(ctx, &cfg, nodes, rels); err != nil {
        log.Fatal(err)
    }
}
```

### Discover Event Facts

```go
facts, err := k8sgraph.DiscoverEventFacts(ctx, cfg)
if err != nil {
    log.Fatal(err)
}

// Write facts to Zep
if err := k8sgraph.WriteZepFacts(ctx, &cfg, facts); err != nil {
    log.Fatal(err)
}
```

## Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `Neo4jURI` | Neo4j connection URI | - |
| `Neo4jUser` | Neo4j username | - |
| `Neo4jPassword` | Neo4j password | - |
| `Neo4jToken` | Neo4j bearer token (alternative auth) | - |
| `Neo4jDatabase` | Neo4j database name | - |
| `ZepAPIKey` | Zep API key | - |
| `ZepGraphID` | Zep graph ID | auto-generated |
| `ZepGraphName` | Zep graph display name | "k8s-graph" |
| `Kubeconfig` | Path to kubeconfig file | in-cluster or ~/.kube/config |
| `Namespaces` | List of namespaces to scan | all |
| `AllNamespaces` | Scan all namespaces | true |
| `MaxPerResource` | Max items per resource type | 0 (unlimited) |
| `EventTypes` | Event types to include | all |
| `MaxEventFacts` | Max event facts to collect | 0 (unlimited) |
| `IncludeStatus` | Include resource status | false |
| `MaxStatusConditions` | Max conditions per resource | 5 |
| `DryRun` | Don't write to backend | false |
| `Debug` | Enable debug logging | false |

## License

MIT
