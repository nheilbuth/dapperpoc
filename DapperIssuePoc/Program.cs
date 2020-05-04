using Dapper;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace DapperIssuePoc
{
    class Program
    {
        private static readonly string connectionString = "";

        public static async Task Main(string[] args)
        {
            await Setup();

            var sql = "SELECT * FROM entitya LEFT JOIN entityb ON entitya.entitybid = entityb.id";

            QuerySyncBuffered(sql);
            QuerySyncNonBuffered(sql);
            await QueryAsyncBuffered(sql);
            await QueryAsyncNonBuffered(sql);
        }

        private static void QuerySyncBuffered(string sql)
        {
            try
            {
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    var results = connection.Query<EntityA, EntityB, EntityA>(
                        sql,
                        (a, b) =>
                        {
                            a.EntityB = b;

                            return a;
                        },
                        buffered: true
                    );

                    foreach (var result in results)
                    {
                        Console.WriteLine("Query buffered works");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query buffered does not work");
            }
        }

        private static void QuerySyncNonBuffered(string sql)
        {
            try
            {
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    var results = connection.Query<EntityA, EntityB, EntityA>(
                        sql,
                        (a, b) =>
                        {
                            a.EntityB = b;

                            return a;
                        },
                        buffered: false
                    );

                    foreach (var result in results)
                    {
                        Console.WriteLine("Query non-buffered works");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Query non-buffered does not work");
            }
        }

        private static async Task QueryAsyncBuffered(string sql)
        {
            try
            {
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    var results = await connection.QueryAsync<EntityA, EntityB, EntityA>(
                        new CommandDefinition(sql, flags: CommandFlags.Buffered),
                        (a, b) =>
                        {
                            a.EntityB = b;

                            return a;
                        }
                    );

                    foreach (var result in results)
                    {
                        Console.WriteLine("QueryAsync buffered works");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("QueryAsync buffered does not work");
            }
        }

        private static async Task QueryAsyncNonBuffered(string sql)
        {
            try
            {
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    var results = await connection.QueryAsync<EntityA, EntityB, EntityA>(
                        new CommandDefinition(sql, flags: CommandFlags.None),
                        (a, b) =>
                        {
                            a.EntityB = b;

                            return a;
                        }
                    );

                    foreach (var result in results)
                    {
                        Console.WriteLine("QueryAsync non-buffered works");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("QueryAsync non-buffered does not work");
            }
        }

        private static async Task Setup()
        {
            using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await connection.ExecuteAsync("DROP TABLE entitya");
                await connection.ExecuteAsync("CREATE TABLE entitya (id uuid NOT NULL PRIMARY KEY, name TEXT, entitybid uuid)");
                await connection.ExecuteAsync("DROP TABLE entityb");
                await connection.ExecuteAsync("CREATE TABLE entityb (id uuid NOT NULL PRIMARY KEY, name TEXT)");

                var bId = Guid.NewGuid();
                await connection.ExecuteAsync(
                    "INSERT INTO entityb VALUES (:id, :name)",
                    new { id = bId, name = "Test" }
                );
                await connection.ExecuteAsync(
                    "INSERT INTO entitya VALUES (:id, :name, :bId)",
                    new { id = Guid.NewGuid(), name = "Test", bId }
                );
            }
        }
    }

    public class EntityA
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public Guid EntityBId { get; set; }
        public EntityB EntityB { get; set; }
    }

    public class EntityB
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}
