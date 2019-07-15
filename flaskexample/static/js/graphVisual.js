		var viz;
		function draw() {
			var config = {
				container_id: "viz",
				server_url: "bolt://ec2-52-90-203-215.compute-1.amazonaws.com:7687",
				server_user: "neo4j",
				server_password: "sq@V$L5#S6DxRDGz",
				labels: {
					"Tag": {
						"caption": "tag_name"
					}
				},
				relationships: {
					"IN_THE_SAME_PICTURE": {
						"thickness": "weight",
						"caption": false
					}
				},
				initial_cypher: "MATCH (n)-[r]->(m) WHERE r.weight > 200 RETURN n,r,m LIMIT 10"
			};
			viz = new NeoVis.default(config);
			viz.render();
			console.log(viz);
		}
