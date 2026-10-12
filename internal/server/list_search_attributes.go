// The list_search_attributes tool, which lists the search attributes a target's Visibility
// queries can use. Temporal Cloud refuses OperatorService calls from customer credentials,
// so on Cloud the custom attributes come from the Cloud Ops API, which takes only API keys.

package server

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/grpc"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type listSearchAttributesInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target"`
}

type searchAttributeList struct {
	SearchAttributes []searchAttributeInfo `json:"search_attributes"`
}

type searchAttributeInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Category string `json:"category"`
}

// cloudSystemSearchAttributes are the system search attributes, which the Cloud Ops API does not list, from the Temporal server's own definitions.
var cloudSystemSearchAttributes = func() map[string]enumspb.IndexedValueType {
	attrs := sadefs.System()
	maps.Copy(attrs, sadefs.Predefined())
	return attrs
}()

// searchAttributeNames keeps the names of each target's search attributes from its last listing.
type searchAttributeNames struct {
	mu       sync.Mutex
	byTarget map[string]map[string]bool
}

func (n *searchAttributeNames) has(target, name string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.byTarget[target][name]
}

func (n *searchAttributeNames) store(target string, attrs ...map[string]enumspb.IndexedValueType) {
	names := map[string]bool{}
	for _, a := range attrs {
		for name := range a {
			names[name] = true
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.byTarget == nil {
		n.byTarget = map[string]map[string]bool{}
	}
	n.byTarget[target] = names
}

func listSearchAttributes(cfg *config.Config, clients *temporal.Clients, names *searchAttributeNames) mcp.ToolHandlerFor[listSearchAttributesInput, envelope[searchAttributeList]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in listSearchAttributesInput) (*mcp.CallToolResult, envelope[searchAttributeList], error) {
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, envelope[searchAttributeList]{}, err
		}
		custom, system, complete, err := fetchSearchAttributes(ctx, clients, names, t)
		if err != nil {
			return nil, envelope[searchAttributeList]{}, err
		}
		list := searchAttributeList{SearchAttributes: []searchAttributeInfo{}}
		for category, attrs := range map[string]map[string]enumspb.IndexedValueType{"custom": custom, "system": system} {
			for name, typ := range attrs {
				list.SearchAttributes = append(list.SearchAttributes, searchAttributeInfo{Name: name, Type: typ.String(), Category: category})
			}
		}
		slices.SortFunc(list.SearchAttributes, func(a, b searchAttributeInfo) int {
			return cmp.Or(strings.Compare(a.Category, b.Category), strings.Compare(a.Name, b.Name))
		})
		return nil, envelope[searchAttributeList]{Target: &t.Name, Result: list, Complete: complete}, nil
	}
}

// fetchSearchAttributes returns t's custom and system search attributes and whether that is all of them, and stores
// their names in names only when it is.
func fetchSearchAttributes(ctx context.Context, clients *temporal.Clients, names *searchAttributeNames, t *config.Target) (custom, system map[string]enumspb.IndexedValueType, complete bool, err error) {
	switch ops := clients.CloudOps(t.Name); {
	case ops != nil:
		resp, err := callServer(ctx, t, cloudOpsAPI, func(ctx context.Context, opts ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error) {
			return ops.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: t.Namespace}, opts...)
		})
		if err != nil {
			return nil, nil, false, err
		}
		custom, system = map[string]enumspb.IndexedValueType{}, cloudSystemSearchAttributes
		for name, typ := range resp.GetNamespace().GetSpec().GetSearchAttributes() {
			// The Cloud Ops API's type names are the IndexedValueType names with another prefix.
			custom[name] = enumspb.IndexedValueType(enumspb.IndexedValueType_value["INDEXED_VALUE_TYPE_"+strings.TrimPrefix(typ.String(), "SEARCH_ATTRIBUTE_TYPE_")])
		}
	case temporal.IsCloud(t.Address):
		return map[string]enumspb.IndexedValueType{}, cloudSystemSearchAttributes, false, nil
	default:
		resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*operatorservice.ListSearchAttributesResponse, error) {
			return clients.Client(t.Name).OperatorService().ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: t.Namespace}, opts...)
		})
		if err != nil {
			return nil, nil, false, err
		}
		custom, system = resp.GetCustomAttributes(), resp.GetSystemAttributes()
	}
	names.store(t.Name, custom, system)
	return custom, system, true, nil
}
