/* Define if your processor stores words with the most significant
   byte first (like Motorola and SPARC, unlike Intel and VAX).  */
/* #undef WORDS_BIGENDIAN */

/* Define if we have the timegm() function */
#define HAVE_TIMEGM

/* Define if we have the zlib library for gzip */
#define HAVE_ZLIB

/* Define if we have the bz2 library for bzip2 */
#define HAVE_BZ2

/* Define if we have the lzma library for lzma/xz */
#define HAVE_LZMA

/* Define if we have the lz4 library for lz4 */
#define HAVE_LZ4

/* Define if we have the zstd library for zst */
#define HAVE_ZSTD

/* Define if we have the systemd library */
#define HAVE_SYSTEMD

/* Define if we have the udev library */
#define HAVE_UDEV

/* Define if we have the seccomp library */
/* #undef HAVE_SECCOMP */

/* These two are used by the statvfs shim for glibc2.0 and bsd */
/* Define if we have sys/vfs.h */
#define HAVE_VFS_H
#define HAVE_STRUCT_STATFS_F_TYPE

/* Define if we have sys/mount.h */
/* #undef HAVE_MOUNT_H */

/* Define if we have sys/endian.h */
/* #undef HAVE_SYS_ENDIAN_H */

/* Define if we have machine/endian.h */
/* #undef HAVE_MACHINE_ENDIAN_H */

/* Check for getresuid() function and similar ones */
#define HAVE_GETRESUID
#define HAVE_GETRESGID
#define HAVE_SETRESUID
#define HAVE_SETRESGID

/* Check for ptsname_r() */
#define HAVE_PTSNAME_R

/* Define the arch name string */
#define COMMON_ARCH "amd64"

/* The package name string */
#define PACKAGE "apt"

/* The version number string */
#define PACKAGE_VERSION "3.1.16"

/* The mail address to reach upstream */
#define PACKAGE_MAIL "APT Development Team <deity@lists.debian.org>"

/* Guard for code that should only be emitted when compiling apt */
#define APT_COMPILING_APT

/* Various directories */
#define CMAKE_INSTALL_FULL_BINDIR "/usr/local/bin"
#define STATE_DIR "/usr/local/var/lib/apt"
#define CACHE_DIR "/usr/local/var/cache/apt"
#define LOG_DIR "/usr/local/var/log/apt"
#define CONF_DIR "/usr/local/etc/apt"
#define LIBEXEC_DIR "/usr/local/lib/apt"
#define BIN_DIR "/usr/local/bin"
#define DPKG_DATADIR "/usr/share/dpkg"

/* Group of the root user */
#define ROOT_GROUP "root"

/* Whether to check for and require merged usr if a usrmerge package is available. */
#define REQUIRE_MERGED_USR

#define DEFAULT_PAGER "pager"
#define PAGER_ENV "LESS=FRSXMK\nMORE=FRX\nLV=-c"
#define SQV_EXECUTABLE "/usr/bin/sqv"
