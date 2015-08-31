create table outlet(
	 id           integer primary key autoincrement not null,
	 name		  text
);

create table podcast(
 title		  text not null,
 link		  text primary key not null,
 summary	  text,
 pubDate	  text not null,
 f_outlet	  integer not null references outlet(id) 
 );


insert into outlet values (1,"abendschau"),(2,"tagesschauShort"),(3,"tagesschau");