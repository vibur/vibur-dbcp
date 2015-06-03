Vibur DBCP is a concurrent, fast, and fully-featured JDBC connection pool, which provides a non-starvation 
guarantee for application threads, statement caching, slow SQL queries detection and logging, 
and Hibernate integration, among other features.

The project [home page](http://www.vibur.org/) contains a full list of its features, including various configuration
examples with Hibernate and Spring, all configuration options, and more.

Vibur DBCP is built on top of [Vibur Object Pool](https://github.com/vibur/vibur-object-pool) - a general-purpose 
concurrent Java object pool.

The project maven coordinates are:

```
<dependency>
  <groupId>org.vibur</groupId>
  <artifactId>vibur-dbcp</artifactId>
  <version>1.2.0</version>
</dependency>   
```

[Originally released](https://raw.githubusercontent.com/vibur/vibur-dbcp/master/CHANGELOG) in July 2013 on 
code.google.com, the project was migrated to GitHub in March 2015.
