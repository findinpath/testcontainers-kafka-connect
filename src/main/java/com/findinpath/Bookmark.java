package com.findinpath;

import java.time.Instant;

/**
 * This class models a bookmark database entity.
 */
public class Bookmark {
    private Long id;

    private String name;

    private String url;

    private Instant updated;

    public Bookmark() {
    }

    public Bookmark(Long id, String name, String url, Instant updated) {
        this.id = id;
        this.name = name;
        this.url = url;
        this.updated = updated;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        return "Bookmark{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", updated=" + updated +
                '}';
    }
}
