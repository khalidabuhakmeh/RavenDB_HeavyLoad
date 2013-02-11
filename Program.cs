using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace RavenLoadTest
{
    public static class Tag
    {
        public static Random Rand = new Random(new DateTime().Millisecond);

        public static IList<string> All = new List<string> {
            "agastopia", "bibble", "cabotage", "doodle sack", "erinaceous", "firman",
            "gabelle", "halfpace", "impignorate", "jetacular", "kakorrhapinophobia",
            "lamprohpony", "marcrosmatic", "nudiustertian", "oxter", "pauciloquent",
            "quire", "ratoon", "salopettes", "tittynope", "ulotrichous", "vapid",
            "winklepicker", "xertz", "yarborough", "zoanthropy"
        };

        public static IList<string> Random(int count = 10)
        {
            return All.OrderBy(x => Rand.Next()).Take(count).ToList();
        }
    }


    public class Trouble
    {
        public Trouble()
        {
            Tags = Tag.Random();
            Email = Guid.NewGuid().ToString("N") + "@email.com";
        }

        public string Id { get; set; }
        public string Email { get; set; }
        public IList<string> Tags { get; set; }
    }

    public class Bubble : Trouble
    {
        public Bubble()
        {
            Code = Guid.NewGuid().ToString("N");
        }

        public string Code { get; set; }
    }

    // Simple Index
    public class Trouble_ByEmail : AbstractIndexCreationTask<Trouble, Trouble_ByEmail.Result>
    {
        public class Result
        {
            public string Email { get; set; }
        }

        public Trouble_ByEmail()
        {
            Map = troubles => from trouble in troubles
                              select new Result { Email = trouble.Email };
        }
    }

    // multi map
    public class BubbleTrouble_Search : AbstractMultiMapIndexCreationTask<BubbleTrouble_Search.Result>
    {
        public class Result
        {
            public object[] Search { get; set; }
        }

        public BubbleTrouble_Search()
        {
            AddMap<Bubble>(bubbles => from bubble in bubbles
                                      select new Result
                                      {
                                          Search = new object[] { bubble.Email, bubble.Code }
                                      });

            AddMap<Trouble>(troubles => from trouble in troubles
                                        select new Result
                                        {
                                            Search = new object[] { @trouble.Email }
                                        });

            Index(x => x.Search, FieldIndexing.Analyzed);
        }
    }

    // multi-map reduce
    public class BubbleTrouble_ByTags : AbstractMultiMapIndexCreationTask<BubbleTrouble_ByTags.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public BubbleTrouble_ByTags()
        {
            AddMap<Bubble>(bubbles => from bubble in bubbles
                                      from tag in bubble.Tags
                                      select new Result
                                      {
                                          Name = tag,
                                          Count = 1
                                      });


            AddMap<Trouble>(troubles => from trouble in troubles
                                        from tag in trouble.Tags
                                        select new Result
                                        {
                                            Name = tag,
                                            Count = 1
                                        });

            Reduce = results => from result in results
                                group result by result.Name
                                    into g
                                    select new Result
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
        }
    }

    class Program
    {
        public static readonly IDocumentStore Store = new DocumentStore
        {
            ConnectionStringName = "RavenDB"
        }.Initialize();

        static void Main(string[] args)
        {
            IndexCreation.CreateIndexes(typeof(Bubble).Assembly, Store);
            var bubbles = new List<PutCommandData>();
            var troubles = new List<PutCommandData>();

            for (int batch = 1; batch <= 100; batch++)
            {
                // let's bulk insert 
                for (int i = 1; i <= 5000; i++)
                {
                    bubbles.Add(
                        new PutCommandData
                        {
                            Document = RavenJObject.FromObject(new Bubble()),
                            Etag = null,
                            Key = string.Format("bubbles/{0}/{1}", batch, i),
                            Metadata = new RavenJObject(),
                        });
                }

                Store.DatabaseCommands.Batch(bubbles);

                for (int i = 1; i <= 5000; i++)
                {
                    troubles.Add(
                        new PutCommandData
                        {
                            Document = RavenJObject.FromObject(new Trouble()),
                            Etag = null,
                            Key = string.Format("troubles/{0}/{1}", batch, i),
                            Metadata = new RavenJObject(),
                        });
                }

                Store.DatabaseCommands.Batch(troubles);

                bubbles.Clear();
                troubles.Clear();

                Console.WriteLine("Batch {0} of 100", batch);
            }

            Console.WriteLine("Inserted a million items");
            Console.ReadLine();
        }
    }
}
