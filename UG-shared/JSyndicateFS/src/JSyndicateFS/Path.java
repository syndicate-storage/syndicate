/*
 * Path class for JSyndicateFS
 */
package JSyndicateFS;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

/**
 *
 * @author iychoi
 */
public class Path implements Comparable {

    // stores path hierarchy
    private URI uri;
    
    /*
     * Construct a path from parent/child pairs
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }
    
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }
    
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }
    
    public Path(Path parent, Path child) {
        if (parent == null)
            throw new IllegalArgumentException("Can not resolve a path from a null parent");
        if (child == null)
            throw new IllegalArgumentException("Can not resolve a path from a null child");
        
        URI parentUri = parent.uri;
        if (parentUri == null)
            throw new IllegalArgumentException("Can not resolve a path from a null parent URI");
        
        String parentPath = parentUri.getPath();
        
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            // parent path is not empty -- need to parse
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null, parentUri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        
        URI resolved = parentUri.resolve(child.uri);
        
        // assign resolved uri to member field
        this.uri = createPathUri(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
    }
    
    /*
     * Construct a path from string
     */
    public Path(String path) {
        if (path == null)
            throw new IllegalArgumentException("Can not create a path from a null string");
        if (path.length() == 0)
            throw new IllegalArgumentException("Can not create a path from an empty string");
        
        String uriScheme = null;
        String uriAuthority = null;
        String uriPath = null;
        
        int start = 0;

        // parse uri scheme
        int colon = path.indexOf(':');
        if (colon != -1) {
            uriScheme = path.substring(0, colon);
            start = colon + 1;
            
            // parse uri authority
            if (path.startsWith("//", start)
                && (path.length() - start > 2)) {
                // have authority
                int nextSlash = path.indexOf('/', start + 2);
                int authEnd;
                if (nextSlash != -1)
                    authEnd = nextSlash;
                else 
                    authEnd = path.length();
                
                uriAuthority = path.substring(start + 2, authEnd);
                start = authEnd;
            }
        }
        
        // uri path
        if(start < path.length())
            uriPath = path.substring(start, path.length());

        // assign resolved uri to member field
        this.uri = createPathUri(uriScheme, uriAuthority, uriPath);
    }
    
    /*
     * Construct a path from URI
     */
    public Path(URI uri) {
        this.uri = uri;
    }
    
    private URI createPathUri(String scheme, String authority, String path) {
        try {
            URI uri = new URI(scheme, authority, normalizePath(path), null, null);
            return uri.normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    private String normalizePath(String path) {
        // replace all "//" and "\" to "/"
        path = path.replace("//", "/");
        path = path.replace("\\", "/");

        // trim trailing slash
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }
    
    /*
     * Return URI formatted path
     */
    public URI toUri() {
        return this.uri;
    }
    
    /*
     * True if the path is absolute
     */
    public boolean isAbsolute() {
        return this.uri.getPath().startsWith("/");
    }
    
    /*
     * Return file name
     */
    public String getName() {
        String path = this.uri.getPath();
        int slash = path.lastIndexOf('/');
        return path.substring(slash + 1, path.length());
    }
    
    /*
     * Return the parent path, Null if parent is root
     */
    public Path getParent() {
        String path = this.uri.getPath();
        int lastSlash = path.lastIndexOf('/');
        // empty
        if (path.length() == 0)
            return null;
        // root
        if (path.length() == 1 && lastSlash == 0)
            return null;
        
        if (lastSlash == -1) {
            return new Path(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), "."));
        } else if (lastSlash == 0) {
            return new Path(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), "/"));
        } else {
            String parent = path.substring(0, lastSlash);
            return new Path(createPathUri(this.uri.getScheme(), this.uri.getAuthority(), parent));
        }
    }
    
    /*
     * Return the stringfied path that contains scheme, authority and path
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (uri.getScheme() != null) {
            sb.append(uri.getScheme());
            sb.append(":");
        }
        if (uri.getAuthority() != null) {
            sb.append("//");
            sb.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            sb.append(path);
        }
        
        return sb.toString();
    }
    
    /*
     * Return the stringfied path that does not contains scheme and authority
     */
    public String getPath() {
        StringBuilder sb = new StringBuilder();
        
        if (uri.getPath() != null) {
            String path = uri.getPath();
            sb.append(path);
        }
        
        return sb.toString();
    }
    
    public int depth() {
        String path = uri.getPath();
        
        if(path.length() == 1 && path.startsWith("/"))
            return 0;
        
        int depth = 0;
        int slash = 0;
        
        while(slash != -1) {
            // slash starts from 1
            slash = path.indexOf("/", slash + 1);
            depth++;
        }
        
        return depth;
    }
    
    public Path[] getAncestors() {
        ArrayList<Path> ancestors = new ArrayList<Path>();
        
        Path parent = getParent();
        while(parent != null) {
            ancestors.add(0, parent);
            
            parent = parent.getParent();
        }
        
        Path[] ancestors_array = new Path[ancestors.size()];
        ancestors_array = ancestors.toArray(ancestors_array);
        return ancestors_array;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path))
            return false;
        
        Path other = (Path) o;
        return this.uri.equals(other.uri);
    }
    
    @Override
    public int hashCode() {
        return this.uri.hashCode();
    }
    
    @Override
    public int compareTo(Object o) {
        Path other = (Path) o;
        return this.uri.compareTo(other.uri);
    }
}
