CREATE TABLE bookmarks(
    id bigserial,
    name varchar(256),
    url varchar(1024),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);
