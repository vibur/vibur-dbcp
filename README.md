<img align="left" src="http://www.vibur.org/img/vibur-130x130.png" alt="Vibur logo"> 
Vibur DBCP is concurrent, fast, and fully-featured JDBC connection pool, which provides advanced performance 
monitoring capabilities, including slow SQL queries detection and logging, a non-starvation guarantee for 
application threads, statement caching, and Hibernate integration, among other features.

The project [home page](http://www.vibur.org/) contains a detailed explanation of all Vibur features and
configuration options, various configuration examples with Hibernate and Spring, and more.

Vibur DBCP is built on top of [Vibur Object Pool](https://github.com/vibur/vibur-object-pool) - a general-purpose 
concurrent Java object pool.

The project maven coordinates are:

```
<dependency>
  <groupId>org.vibur</groupId>
  <artifactId>vibur-dbcp</artifactId>
  <version>17.0</version>
</dependency>   
```

[Originally released](https://raw.githubusercontent.com/vibur/vibur-dbcp/master/CHANGELOG) in July 2013 on 
code.google.com, the project was migrated to GitHub in March 2015.
