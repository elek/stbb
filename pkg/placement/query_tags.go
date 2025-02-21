package placement

import (
	"fmt"
	"strings"
)

type QueryTags struct {
}

func (q QueryTags) Run() error {
	query := "WITH\n"

	tags := map[string]bool{
		"soc2": true, "operator": true, "owner": true, "vivint-exclude-upload": true, "surge": true, "us-select-exclude-upload": true,
		"host": false, "service": false, "server_group": false,
	}

	for tag, authoritive := range tags {

		signer := "FROM_HEX('0000000000000000000000000000000000000000000000000000000000000100')"
		if !authoritive {
			signer = "node_id"
		}
		query += fmt.Sprintf("%s as (select node_id, name, safe_convert_bytes_to_string(value) as strvalue  from node_tags where name = '%s' and signer = %s),\n", strings.ReplaceAll(tag, "-", "_"), tag, signer)
	}

	query += "\nnode_with_tags as (\n    select nodes.*,\n"
	for tag, _ := range tags {
		tag = strings.ReplaceAll(tag, "-", "_")
		query += fmt.Sprintf("    %s.strvalue as %s,\n", tag, tag)
	}
	query = query[:len(query)-2]
	query += "\nfrom nodes\n"
	for tag, _ := range tags {
		tag = strings.ReplaceAll(tag, "-", "_")
		query += fmt.Sprintf("    left join %s on %s.node_id = nodes.id\n", tag, tag)
	}
	query += ")\nselect * from  node_with_tags\n"
	fmt.Println(query)
	return nil
}
