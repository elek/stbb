package placement

import (
	"fmt"
	"strings"
)

type QueryTags struct {
	Tags           []string `help:"the tags to query for, separated by commas"`
	SelfSignedTags []string `help:"the self signed tags to query for, separated by commas"`
	QueryEnd       string   `help:"the query end, default is empty"`
	Fields         []string `help:"the node table fields to return (node_id is always added)" default:""`
}

func (q QueryTags) Run() error {
	query := "WITH\n"

	tags := map[string]bool{}
	for _, t := range q.Tags {
		tags[t] = true
	}
	for _, t := range q.SelfSignedTags {
		tags[t] = false
	}

	what := "node_with_tags.id as node_id"
	for _, field := range q.Fields {
		what += ",node_with_tags." + field + " as " + field
	}

	for tag, authoritive := range tags {
		signer := "FROM_HEX('0000000000000000000000000000000000000000000000000000000000000100')"
		if !authoritive {
			signer = "node_id"
		}
		query += fmt.Sprintf("%s as (select node_id, name, safe_convert_bytes_to_string(value) as strvalue  from node_tags where name = '%s' and signer = %s),\n", strings.ReplaceAll(tag, "-", "_"), tag, signer)
		what += "," + strings.ReplaceAll(tag, "-", "_")
	}

	query += "\nnode_with_tags as (\n    select nodes.*,\n"
	for tag := range tags {
		tag = strings.ReplaceAll(tag, "-", "_")
		query += fmt.Sprintf("    %s.strvalue as %s,\n", tag, tag)
	}
	query = query[:len(query)-2]
	query += "\nfrom nodes\n"
	for tag := range tags {
		tag = strings.ReplaceAll(tag, "-", "_")
		query += fmt.Sprintf("    left join %s on %s.node_id = nodes.id\n", tag, tag)
	}
	query += ")\nselect " + what + " from  node_with_tags " + q.QueryEnd + "\n"

	fmt.Println(query)
	return nil
}
