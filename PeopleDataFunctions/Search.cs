using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace PeopleDataFunctions
{
    public static class Search
    {
        static string endpoint = ConfigurationManager.AppSettings["Endpoint"];
        static string authKey = ConfigurationManager.AppSettings["AuthKey"];

        [FunctionName("Search")]
        public static async Task<HttpResponseMessage> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]HttpRequestMessage req,
           TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // the person objects will be free-form in structure
            List<dynamic> results = new List<dynamic>();

            // open the client's connection
            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint),
                authKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                }))
            {
                // get a reference to the database the console app created
                Database database = await client.CreateDatabaseIfNotExistsAsync(
                    new Database
                    {
                        Id = "graphdb"
                    });

                // get an instance of the database's graph
                DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri("graphdb"),
                    new DocumentCollection { Id = "graphcollz" },
                    new RequestOptions { OfferThroughput = 1000 });

                // build a gremlin query based on the existence of a name parameter
                string name = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
                    .Value;

                IDocumentQuery<dynamic> query = (!String.IsNullOrEmpty(name))
                    ? client.CreateGremlinQuery<dynamic>(graph, string.Format("g.V('{0}')", name))
                    : client.CreateGremlinQuery<dynamic>(graph, "g.V()");

                // iterate over all the results and add them to the list
                while (query.HasMoreResults)
                    foreach (dynamic result in await query.ExecuteNextAsync())
                        results.Add(result);
            }

            // return the list with an OK response
            return req.CreateResponse<List<dynamic>>(HttpStatusCode.OK, results);
        }
    }
}
