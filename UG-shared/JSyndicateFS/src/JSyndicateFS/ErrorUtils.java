/*
 * ErrorUtil class for JSyndicateFS
 */
package JSyndicateFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
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
        putErrorEntry(1, "EPERM", "Operation not permitted", IOException.class);
        putErrorEntry(2, "ENOENT", "No such file or directory", FileNotFoundException.class);
        putErrorEntry(3, "ESRCH", "No such process", IOException.class);
        putErrorEntry(4, "EINTR", "Interrupted system call", InterruptedException.class);
        putErrorEntry(5, "EIO", "I/O error", IOException.class);
        putErrorEntry(6, "ENXIO", "No such device or address", IOException.class);
        putErrorEntry(7, "E2BIG", "Arg list too long", IOException.class);
        putErrorEntry(8, "ENOEXEC", "Exec format error", IOException.class);
        putErrorEntry(9, "EBADF", "Bad file number", IOException.class);
        putErrorEntry(10, "ECHILD", "No child processes", IOException.class);
        putErrorEntry(11, "EAGAIN", "Try again", IOException.class);
        putErrorEntry(12, "ENOMEM", "Out of memory", IOException.class);
        putErrorEntry(13, "EACCES", "Permission denied", IOException.class);
        putErrorEntry(14, "EFAULT", "Bad address", IOException.class);
        putErrorEntry(15, "ENOTBLK", "Block device required", IOException.class);
        putErrorEntry(16, "EBUSY", "Device or resource busy", IOException.class);
        putErrorEntry(17, "EEXIST", "File exists", IOException.class);
        putErrorEntry(18, "EXDEV", "Cross-device link", IOException.class);
        putErrorEntry(19, "ENODEV", "No such device", IOException.class);
        putErrorEntry(20, "ENOTDIR", "Not a directory", IOException.class);
        putErrorEntry(21, "EISDIR", "Is a directory", IOException.class);
        putErrorEntry(22, "EINVAL", "Invalid argument", IOException.class);
        putErrorEntry(23, "ENFILE", "File table overflow", IOException.class);
        putErrorEntry(24, "EMFILE", "Too many open files", IOException.class);
        putErrorEntry(25, "ENOTTY", "Not a typewriter", IOException.class);
        putErrorEntry(26, "ETXTBSY", "Text file busy", IOException.class);
        putErrorEntry(27, "EFBIG", "File too large", IOException.class);
        putErrorEntry(28, "ENOSPC", "No space left on device", IOException.class);
        putErrorEntry(29, "ESPIPE", "Illegal seek", IOException.class);
        putErrorEntry(30, "EROFS", "Read-only file system", IOException.class);
        putErrorEntry(31, "EMLINK", "Too many links", IOException.class);
        putErrorEntry(32, "EPIPE", "Broken pipe", IOException.class);
        putErrorEntry(33, "EDOM", "Math argument out of domain of func", IOException.class);
        putErrorEntry(34, "ERANGE", "Math result not representable", IOException.class);
        putErrorEntry(35, "EDEADLK", "Resource deadlock would occur", IOException.class);
        putErrorEntry(36, "ENAMETOOLONG", "File name too long", IOException.class);
        putErrorEntry(37, "ENOLCK", "No record locks available", IOException.class);
        putErrorEntry(38, "ENOSYS", "Function not implemented", IOException.class);
        putErrorEntry(39, "ENOTEMPTY", "Directory not empty", IOException.class);
        putErrorEntry(40, "ELOOP", "Too many symbolic links encountered", IOException.class);
        putErrorEntry(42, "ENOMSG", "No message of desired type", IOException.class);
        putErrorEntry(43, "EIDRM", "Identifier removed", IOException.class);
        putErrorEntry(44, "ECHRNG", "Channel number out of range", IOException.class);
        putErrorEntry(45, "EL2NSYNC", "Level 2 not synchronized", IOException.class);
        putErrorEntry(46, "EL3HLT", "Level 3 halted", IOException.class);
        putErrorEntry(47, "EL3RST", "Level 3 reset", IOException.class);
        putErrorEntry(48, "ELNRNG", "Link number out of range", IOException.class);
        putErrorEntry(49, "EUNATCH", "Protocol driver not attached", IOException.class);
        putErrorEntry(50, "ENOCSI", "No CSI structure available", IOException.class);
        putErrorEntry(51, "EL2HLT", "Level 2 halted", IOException.class);
        putErrorEntry(52, "EBADE", "Invalid exchange", IOException.class);
        putErrorEntry(53, "EBADR", "Invalid request descriptor", IOException.class);
        putErrorEntry(54, "EXFULL", "Exchange full", IOException.class);
        putErrorEntry(55, "ENOANO", "No anode", IOException.class);
        putErrorEntry(56, "EBADRQC", "Invalid request code", IOException.class);
        putErrorEntry(57, "EBADSLT", "Invalid slot", IOException.class);
        putErrorEntry(59, "EBFONT", "Bad font file format", IOException.class);
        putErrorEntry(60, "ENOSTR", "Device not a stream", IOException.class);
        putErrorEntry(61, "ENODATA", "No data available", IOException.class);
        putErrorEntry(62, "ETIME", "Timer expired", IOException.class);
        putErrorEntry(63, "ENOSR", "Out of streams resources", IOException.class);
        putErrorEntry(64, "ENONET", "Machine is not on the network", IOException.class);
        putErrorEntry(65, "ENOPKG", "Package not installed", IOException.class);
        putErrorEntry(66, "EREMOTE", "Object is remote", IOException.class);
        putErrorEntry(67, "ENOLINK", "Link has been severed", IOException.class);
        putErrorEntry(68, "EADV", "Advertise error", IOException.class);
        putErrorEntry(69, "ESRMNT", "Srmount error", IOException.class);
        putErrorEntry(70, "ECOMM", "Communication error on send", IOException.class);
        putErrorEntry(71, "EPROTO", "Protocol error", IOException.class);
        putErrorEntry(72, "EMULTIHOP", "Multihop attempted", IOException.class);
        putErrorEntry(73, "EDOTDOT", "RFS specific error", IOException.class);
        putErrorEntry(74, "EBADMSG", "Not a data message", IOException.class);
        putErrorEntry(75, "EOVERFLOW", "Value too large for defined data type", IOException.class);
        putErrorEntry(76, "ENOTUNIQ", "Name not unique on network", IOException.class);
        putErrorEntry(77, "EBADFD", "File descriptor in bad state", IOException.class);
        putErrorEntry(78, "EREMCHG", "Remote address changed", IOException.class);
        putErrorEntry(79, "ELIBACC", "Can not access a needed shared library", IOException.class);
        putErrorEntry(80, "ELIBBAD", "Accessing a corrupted shared library", IOException.class);
        putErrorEntry(81, "ELIBSCN", ".lib section in a.out corrupted", IOException.class);
        putErrorEntry(82, "ELIBMAX", "Attempting to link in too many shared libraries", IOException.class);
        putErrorEntry(83, "ELIBEXEC", "Cannot exec a shared library directly", IOException.class);
        putErrorEntry(84, "EILSEQ", "Illegal byte sequence", IOException.class);
        putErrorEntry(85, "ERESTART", "Interrupted system call should be restarted", IOException.class);
        putErrorEntry(86, "ESTRPIPE", "Streams pipe error", IOException.class);
        putErrorEntry(87, "EUSERS", "Too many users", IOException.class);
        putErrorEntry(88, "ENOTSOCK", "Socket operation on non-socket", IOException.class);
        putErrorEntry(89, "EDESTADDRREQ", "Destination address required", IOException.class);
        putErrorEntry(90, "EMSGSIZE", "Message too long", IOException.class);
        putErrorEntry(91, "EPROTOTYPE", "Protocol wrong type for socket", IOException.class);
        putErrorEntry(92, "ENOPROTOOPT", "Protocol not available", IOException.class);
        putErrorEntry(93, "EPROTONOSUPPORT", "Protocol not supported", IOException.class);
        putErrorEntry(94, "ESOCKTNOSUPPORT", "Socket type not supported", IOException.class);
        putErrorEntry(95, "EOPNOTSUPP", "Operation not supported on transport endpoint", IOException.class);
        putErrorEntry(96, "EPFNOSUPPORT", "Protocol family not supported", IOException.class);
        putErrorEntry(97, "EAFNOSUPPORT", "Address family not supported by protocol", IOException.class);
        putErrorEntry(98, "EADDRINUSE", "Address already in use", IOException.class);
        putErrorEntry(99, "EADDRNOTAVAIL", "Cannot assign requested address", IOException.class);
        putErrorEntry(100, "ENETDOWN", "Network is down", IOException.class);
        putErrorEntry(101, "ENETUNREACH", "Network is unreachable", IOException.class);
        putErrorEntry(102, "ENETRESET", "Network dropped connection because of reset", IOException.class);
        putErrorEntry(103, "ECONNABORTED", "Software caused connection abort", IOException.class);
        putErrorEntry(104, "ECONNRESET", "Connection reset by peer", IOException.class);
        putErrorEntry(105, "ENOBUFS", "No buffer space available", IOException.class);
        putErrorEntry(106, "EISCONN", "Transport endpoint is already connected", IOException.class);
        putErrorEntry(107, "ENOTCONN", "Transport endpoint is not connected", IOException.class);
        putErrorEntry(108, "ESHUTDOWN", "Cannot send after transport endpoint shutdown", IOException.class);
        putErrorEntry(109, "ETOOMANYREFS", "Too many references", IOException.class);
        putErrorEntry(110, "ETIMEDOUT", "Connection timed out", IOException.class);
        putErrorEntry(111, "ECONNREFUSED", "Connection refused", IOException.class);
        putErrorEntry(112, "EHOSTDOWN", "Host is down", IOException.class);
        putErrorEntry(113, "EHOSTUNREACH", "No route to host", IOException.class);
        putErrorEntry(114, "EALREADY", "Operation already in progress", IOException.class);
        putErrorEntry(115, "EINPROGRESS", "Operation now in progress", IOException.class);
        putErrorEntry(116, "ESTALE", "Stale NFS file handle", IOException.class);
        putErrorEntry(117, "EUCLEAN", "Structure needs cleaning", IOException.class);
        putErrorEntry(118, "ENOTNAM", "Not a XENIX named type file", IOException.class);
        putErrorEntry(119, "ENAVAIL", "No XENIX semaphores available", IOException.class);
        putErrorEntry(120, "EISNAM", "Is a named type file", IOException.class);
        putErrorEntry(121, "EREMOTEIO", "Remote I/O error", IOException.class);
        putErrorEntry(122, "EDQUOT", "Quota exceeded", IOException.class);
        putErrorEntry(123, "ENOMEDIUM", "No medium found", IOException.class);
        putErrorEntry(124, "EMEDIUMTYPE", "Wrong medium type", IOException.class);
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
                LOG.error("Can not generate Exception from errno : " + errno);
            }
            
            return null;
        } else {
            LOG.error("Cannot find such error number : " + errno);
            
            return null;
        }
    }
}
