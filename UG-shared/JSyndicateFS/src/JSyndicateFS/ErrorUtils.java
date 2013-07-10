/*
 * ErrorUtil class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.FileAlreadyExistsException;
import java.util.Hashtable;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ErrorUtils {
    
    public static final Log LOG = LogFactory.getLog(ErrorUtils.class);
    
    private static class ErrorDetail {
        private int no;
        private String id;
        private String message;
        private Class<? extends Exception> exception;
        
        public ErrorDetail(int no, String id, String message, Class<? extends Exception> exception) {
            this.no = no;
            this.id = id;
            this.message = message;
            this.exception = exception;
        }
        
        public int getNo() {
            return this.no;
        }
        
        public String getID() {
            return this.id;
        }
        
        public String getMessage() {
            return this.message;
        }
        
        public Class<? extends Exception> getExceptionClass() {
            return this.exception;
        }
    }
    
    private static Map<Integer, ErrorDetail> error_map = new Hashtable<Integer, ErrorDetail>();
    
    static {
        initialize();
    }
    
    private static void initialize() {
        putErrorEntry(1, "EPERM", "Operation is not permitted", IOException.class);
        putErrorEntry(2, "ENOENT", "No such file or directory", FileNotFoundException.class);
        putErrorEntry(3, "ESRCH", "No such process", IOException.class);
        putErrorEntry(4, "EINTR", "Interrupted system call", InterruptedException.class);
        putErrorEntry(5, "EIO", "Input/output error", IOException.class);
        putErrorEntry(6, "ENXIO", "Device not configured", IOException.class);
        putErrorEntry(7, "E2BIG", "Argument list too long", IOException.class);
        putErrorEntry(8, "ENOEXEC", "Exec format error", IOException.class);
        putErrorEntry(9, "EBADF", "Bad file descriptor", IOException.class);
        putErrorEntry(10, "ECHILD", "No child processes", IOException.class);
        putErrorEntry(11, "EDEADLK", "Resource deadlock avoided", IOException.class);
        putErrorEntry(12, "ENOMEM", "Cannot allocate memory", IOException.class);
        putErrorEntry(13, "EACCES", "Permission denied", IOException.class);
        putErrorEntry(14, "EFAULT", "Bad address", IOException.class);
        putErrorEntry(15, "ENOTBLK", "Block device required", IOException.class);
        putErrorEntry(16, "EBUSY", "Device busy", IOException.class);
        putErrorEntry(17, "EEXIST", "File exists", FileAlreadyExistsException.class);
        putErrorEntry(18, "EXDEV", "Cross-device link", IOException.class);
        putErrorEntry(19, "ENODEV", "Operation not supported by device", IOException.class);
        putErrorEntry(20, "ENOTDIR", "Not a directory", IOException.class);
        putErrorEntry(21, "EISDIR", "Is a directory", IOException.class);
        putErrorEntry(22, "EINVAL", "Invalid argument", IOException.class);
        putErrorEntry(23, "ENFILE", "Too many open files in system", IOException.class);
        putErrorEntry(24, "EMFILE", "Too many open files", IOException.class);
        putErrorEntry(25, "ENOTTY", "Inappropriate ioctl for device", IOException.class);
        putErrorEntry(26, "ETXTBSY", "Text file busy", IOException.class);
        putErrorEntry(27, "EFBIG", "File too large", IOException.class);
        putErrorEntry(28, "ENOSPC", "No space left on device", IOException.class);
        putErrorEntry(29, "ESPIPE", "Illegal seek", IOException.class);
        putErrorEntry(30, "EROFS", "Read-only file system", IOException.class);
        putErrorEntry(31, "EMLINK", "Too many links", IOException.class);
        putErrorEntry(32, "EPIPE", "Broken pipe", IOException.class);
        putErrorEntry(33, "EDOM", "Numerical argument out of domain", IOException.class);
        putErrorEntry(34, "ERANGE", "Result too large", IOException.class);
        putErrorEntry(35, "EWOULDBLOCK", "Operation would block", IOException.class);
        putErrorEntry(36, "EINPROGRESS", "Operation now in progress", IOException.class);
        putErrorEntry(37, "EALREADY", "Operation already in progress", IOException.class);
        putErrorEntry(38, "ENOTSOCK", "Socket operation on non-socket", IOException.class);
        putErrorEntry(39, "EDESTADDRREQ", "Destination address required", IOException.class);
        putErrorEntry(40, "EMSGSIZE", "Message too long", IOException.class);
        putErrorEntry(41, "EPROTOTYPE", "Protocol wrong type for socket", IOException.class);
        putErrorEntry(42, "ENOPROTOOPT", "Protocol not available", IOException.class);
        putErrorEntry(43, "EPROTONOSUPPORT", "Protocol not supported", IOException.class);
        putErrorEntry(44, "ESOCKTNOSUPPORT", "Socket type not supported", IOException.class);
        putErrorEntry(45, "EOPNOTSUPP", "Operation not supported", IOException.class);
        putErrorEntry(46, "EPFNOSUPPORT", "Protocol family not supported", IOException.class);
        putErrorEntry(47, "EAFNOSUPPORT", "Address family not supported by protocol family", IOException.class);
        putErrorEntry(48, "EADDRINUSE", "Address already in use", IOException.class);
        putErrorEntry(49, "EADDRNOTAVAIL", "Can't assign requested address", IOException.class);
        putErrorEntry(50, "ENETDOWN", "Network is down", IOException.class);
        putErrorEntry(51, "ENETUNREACH", "Network is unreachable", IOException.class);
        putErrorEntry(52, "ENETRESET", "Network dropped connection on reset", IOException.class);
        putErrorEntry(53, "ECONNABORTED", "Software caused connection abort", IOException.class);
        putErrorEntry(54, "ECONNRESET", "Connection reset by peer", IOException.class);
        putErrorEntry(55, "ENOBUFS", "No buffer space available", IOException.class);
        putErrorEntry(56, "EISCONN", "Socket is already connected", IOException.class);
        putErrorEntry(57, "ENOTCONN", "Socket is not connected", IOException.class);
        putErrorEntry(58, "ESHUTDOWN", "Can't send after socket shutdown", IOException.class);
        putErrorEntry(59, "ETOOMANYREFS", "Too many references: can't splice", IOException.class);
        putErrorEntry(60, "ETIMEDOUT", "Operation timed out", IOException.class);
        putErrorEntry(61, "ECONNREFUSED", "Connection refused", IOException.class);
        putErrorEntry(62, "ELOOP", "Too many levels of symbolic links", IOException.class);
        putErrorEntry(63, "ENAMETOOLONG", "File name too long", IOException.class);
        putErrorEntry(64, "EHOSTDOWN", "Host is down", IOException.class);
        putErrorEntry(65, "EHOSTUNREACH", "No route to host", IOException.class);
        putErrorEntry(66, "ENOTEMPTY", "Directory not empty", IOException.class);
        putErrorEntry(67, "EPROCLIM", "Too many processes", IOException.class);
        putErrorEntry(68, "EUSERS", "Too many users", IOException.class);
        putErrorEntry(69, "EDQUOT", "Disk quota exceeded", IOException.class);
        putErrorEntry(70, "ESTALE", "Stale NFS file handle", IOException.class);
        putErrorEntry(71, "EREMOTE", "Too many levels of remote in path", IOException.class);
        putErrorEntry(72, "EBADRPC", "RPC struct is bad", IOException.class);
        putErrorEntry(73, "ERPCMISMATCH", "RPC version wrong", IOException.class);
        putErrorEntry(74, "EPROGUNAVAIL", "RPC prog. not avail", IOException.class);
        putErrorEntry(75, "EPROGMISMATCH", "Program version wrong", IOException.class);
        putErrorEntry(76, "EPROCUNAVAIL", "Bad procedure for program", IOException.class);
        putErrorEntry(77, "ENOLCK", "No locks available", IOException.class);
        putErrorEntry(78, "ENOSYS", "Function not implemented", IOException.class);
        putErrorEntry(79, "EFTYPE", "Inappropriate file type or format", IOException.class);
        putErrorEntry(80, "EAUTH", "Authentication error", IOException.class);
        putErrorEntry(81, "ENEEDAUTH", "Need authenticator", IOException.class);
        putErrorEntry(82, "EIPSEC", "IPsec processing failure", IOException.class);
        putErrorEntry(83, "ENOATTR", "Attribute not found", IOException.class);
        putErrorEntry(84, "EILSEQ", "Illegal byte sequence", IOException.class);
        putErrorEntry(85, "ENOMEDIUM", "No medium found", IOException.class);
        putErrorEntry(86, "EMEDIUMTYPE", "Wrong Medium Type", IOException.class);
        putErrorEntry(87, "EOVERFLOW", "Conversion overflow", IOException.class);
        putErrorEntry(88, "ECANCELED", "Operation canceled", IOException.class);
        putErrorEntry(89, "EIDRM", "Identifier removed", IOException.class);
        putErrorEntry(90, "ENOMSG", "No message of desired type", IOException.class);
    }
    
    private static void putErrorEntry(int errno, String errid, String errmsg, Class<? extends Exception> clazz) {
        error_map.put(errno, new ErrorDetail(errno, errid, errmsg, clazz));
    }
    
    public static String generateErrorMessage(int errno) {
        ErrorDetail detail = error_map.get(Math.abs(errno));
        if(detail != null) {
            return detail.getMessage() + " (id : " + detail.getID() + ", errno : " + detail.getNo() + ")";
        } else {
            return null;
        }
    }
    
    public static Exception generateException(int errno) {
        ErrorDetail detail = error_map.get(Math.abs(errno));
        if(detail != null) {
            String errmsg = generateErrorMessage(errno);
            Constructor constructor;
            try {
                constructor = detail.getExceptionClass().getConstructor(String.class);
                return (Exception)constructor.newInstance(errmsg);
            } catch (Exception ex) {
                LOG.error(ex);
            }
            
            return null;
        } else {
            LOG.error("Cannot find such error number : " + errno);
            
            return null;
        }
    }
}
