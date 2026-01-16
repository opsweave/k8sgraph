package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/opsweave/k8sgraph"
)

func main() {
	ctx := context.Background()

	cfg := k8sgraph.Config{
		Kubeconfig:    os.Getenv("KUBECONFIG"),
		AllNamespaces: true,
		Debug:         true,
		DryRun:        true, // Don't write to any backend
	}

	// Discover K8s resources
	nodes, rels, err := k8sgraph.DiscoverGraph(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Discovered %d nodes and %d relationships\n", len(nodes), len(rels))

	// Print first 10 nodes
	fmt.Println("\nFirst 10 nodes:")
	for i, node := range nodes {
		if i >= 10 {
			break
		}
		fmt.Printf("  - %s: %s/%s\n",
			node.Props["kind"],
			node.Props["namespace"],
			node.Props["resource_name"],
		)
	}

	// Print relationship types
	relTypes := make(map[string]int)
	for _, rel := range rels {
		relTypes[rel.Type]++
	}
	fmt.Println("\nRelationship types:")
	for t, count := range relTypes {
		fmt.Printf("  - %s: %d\n", t, count)
	}
}
