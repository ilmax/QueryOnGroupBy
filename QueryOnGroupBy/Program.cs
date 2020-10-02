using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;

namespace QueryOnGroupBy
{
    class Program
    {
        static void Main(string[] args)
        {
            using var context = new Context();
            context.Database.EnsureDeleted();
            context.Database.EnsureCreated();
            context.SaveChanges();

            var groupedQuery = context.EntityWithChildren.GroupBy(k => new { k.Id })
                .Select(x => new { x.Key.Id, Version = x.Max(v => v.Version) });

            var maxVersionQuery = context.EntityWithChildren.Join(groupedQuery,
                l => new { l.Id, l.Version }, r => new { r.Id, r.Version }, (l, r) => l);

            var entities = maxVersionQuery.ToList();
            Console.WriteLine($"Entities count: {entities.Count}");

            var maxVersionQuery2 = context.EntityWithChildren.Join(groupedQuery,
                l => new { l.Id, l.Version }, r => new { r.Id, r.Version }, (l, r) => l)
                .Select(x => new { x.Id, x.Version, Children = x.Children.Select(y => y.Id) });

            var entities2 = maxVersionQuery2.ToList();
            Console.WriteLine($"Entities count: {entities2.Count}");
        }
    }

    public class EntityWithChildren
    {
        public Guid Id { get; set; }
        public int Version { get; set; }
        public ICollection<Child> Children { get; set; }
    }

    public class Child
    {
        public int Id { get; set; }
    }

    public class Context : DbContext
    {
        public DbSet<EntityWithChildren> EntityWithChildren { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EntityWithChildren>(e =>
            {
                e.Property(x => x.Id).ValueGeneratedNever();
                e.HasKey(x => new { x.Id, x.Version });

                e.ToTable("EntitiesWithChildren");
            });
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Data Source=.;Initial Catalog=QueryOnGroupBy;Integrated Security=True", opt => opt.UseQuerySplittingBehavior(QuerySplittingBehavior.SingleQuery));
            //optionsBuilder.LogTo(Console.WriteLine);
        }
    }
}
