// Native-only compatibility facade for the legacy import layer.
//
// GeminiOS now keeps only the gpkg package format and repository flow in this
// tree. The symbols below remain as light compatibility shims so the native
// planner and repository code can compile without the old backend.

struct DebianBackendConfig {
    std::string packages_url = "https://repo.creitingameplays.com";
    std::string base_url = "https://repo.creitingameplays.com";
    std::string apt_arch = "amd64";
};

struct DebianPackageRecord {
    std::string package;
    std::string version;
    std::string architecture;
    std::string multi_arch;
    std::string maintainer;
    std::string section;
    std::string priority;
    std::string filename;
    std::string sha256;
    std::string sha512;
    std::string description;
};

struct DebianPackagesCacheState {
    std::string packages_url;
    std::string etag;
    std::string last_modified;
    long content_length = -1;
};

struct DebianImportedIndexCacheState {
    std::string fingerprint;
    size_t package_count = 0;
};

struct DebianCompiledRecordCacheState {
    std::string policy_fingerprint;
    size_t record_count = 0;
};

struct DebianParsedRecordLoadResult {
    std::vector<DebianPackageRecord> records;
    std::vector<std::string> cache_entries;
    size_t reused_records = 0;
    size_t reparsed_records = 0;
};

struct RawDebianAvailabilityResult {
    bool found = false;
    bool installable = false;
    std::string requested_name;
    std::string resolved_name;
    std::string reason;
    PackageMetadata meta;
};

struct RawDebianOffsetIndexEntry {
    uint64_t offset = 0;
    std::string raw_package;
};

struct RawDebianNameIndexEntry {
    std::string key;
    std::vector<std::string> raw_names;
    std::vector<std::string> raw_packages;
};

struct RawDebianContext {
    bool loaded = false;
    bool available = false;
    std::string problem;
    DebianBackendConfig config;
    ImportPolicy policy;
    std::vector<RawDebianOffsetIndexEntry> raw_package_offsets;
    std::vector<RawDebianNameIndexEntry> import_name_to_raw_names;
    std::vector<RawDebianNameIndexEntry> provider_map;
};

struct DebianSearchPreviewEntry {
    PackageMetadata meta;
    bool installable = false;
    std::string reason;
    std::vector<std::string> raw_names;
};

struct DebianCompiledRecordCacheEntry {
    std::string raw_package;
    std::string record_fingerprint;
    std::vector<std::string> provided_symbols;
    bool importable = false;
    std::string skip_reason;
    PackageMetadata meta;
};

struct DebianParsedRecordCacheState {
    std::string fingerprint;
    size_t record_count = 0;
};

struct DebianIncrementalImportResult {
    std::vector<PackageMetadata> entries;
    std::vector<DebianCompiledRecordCacheEntry> compiled_record_entries;
    std::vector<std::string> skipped_policy;
    bool compiled_record_cache_written = false;
    size_t processed_records = 0;
    size_t imported_records = 0;
};

std::string safe_repo_filename_component(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (char ch : value) {
        unsigned char uch = static_cast<unsigned char>(ch);
        if (std::isalnum(uch) || ch == '.' || ch == '-' || ch == '_') out.push_back(ch);
        else out.push_back('_');
    }
    return out;
}

std::string derive_debian_t64_legacy_alias(const std::string& pkg_name) {
    if (pkg_name.size() > 3 && pkg_name.compare(pkg_name.size() - 3, 3, "t64") == 0) {
        return pkg_name.substr(0, pkg_name.size() - 3);
    }
    return "";
}

std::string lowercase_copy(const std::string& value) {
    std::string out = value;
    for (char& ch : out) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return out;
}

std::string extract_http_header_value(
    const std::string& headers,
    const std::string& lower_headers,
    const std::string& needle
) {
    size_t pos = lower_headers.find(needle);
    if (pos == std::string::npos) return "";
    pos += needle.size();
    size_t end = headers.find('\n', pos);
    if (end == std::string::npos) end = headers.size();
    return trim(headers.substr(pos, end - pos));
}

std::string fnv1a64_hex_digest(const std::vector<std::string>& fields) {
    constexpr uint64_t offset_basis = 1469598103934665603ull;
    constexpr uint64_t prime = 1099511628211ull;
    uint64_t hash = offset_basis;
    for (const auto& field : fields) {
        for (unsigned char ch : field) {
            hash ^= static_cast<uint64_t>(ch);
            hash *= prime;
        }
        hash ^= static_cast<uint64_t>('\n');
        hash *= prime;
    }
    std::ostringstream oss;
    oss << std::hex << std::nouppercase << hash;
    return oss.str();
}

std::string sha256_hex_digest(const std::string& value) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(), digest);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned char byte : digest) oss << std::setw(2) << static_cast<unsigned int>(byte);
    return oss.str();
}

std::string sha256_hex_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "";
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    char buffer[8192];
    while (in) {
        in.read(buffer, sizeof(buffer));
        std::streamsize count = in.gcount();
        if (count > 0) SHA256_Update(&ctx, buffer, static_cast<size_t>(count));
    }
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256_Final(digest, &ctx);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned char byte : digest) oss << std::setw(2) << static_cast<unsigned int>(byte);
    return oss.str();
}

std::string json_string_field(const std::string& key, const std::string& value) {
    return "\"" + json_escape(key) + "\":\"" + json_escape(value) + "\"";
}

std::string json_array_from_strings(const std::vector<std::string>& values) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0) oss << ",";
        oss << "\"" << json_escape(values[i]) << "\"";
    }
    oss << "]";
    return oss.str();
}

bool remove_optional_cache_export(const std::string& path) {
    if (path.empty()) return true;
    if (access(path.c_str(), F_OK) != 0) return true;
    return unlink(path.c_str()) == 0;
}

bool write_binary_exact(std::ostream& out, const void* data, size_t size) {
    out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    return static_cast<bool>(out);
}

bool read_binary_exact(std::istream& in, void* data, size_t size) {
    in.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
    return static_cast<size_t>(in.gcount()) == size;
}

bool write_binary_u32(std::ostream& out, uint32_t value) {
    uint32_t le = htole32(value);
    return write_binary_exact(out, &le, sizeof(le));
}

bool read_binary_u32(std::istream& in, uint32_t& value) {
    uint32_t le = 0;
    if (!read_binary_exact(in, &le, sizeof(le))) return false;
    value = le32toh(le);
    return true;
}

bool write_binary_u64(std::ostream& out, uint64_t value) {
    uint64_t le = htole64(value);
    return write_binary_exact(out, &le, sizeof(le));
}

bool read_binary_u64(std::istream& in, uint64_t& value) {
    uint64_t le = 0;
    if (!read_binary_exact(in, &le, sizeof(le))) return false;
    value = le64toh(le);
    return true;
}

bool write_binary_string(std::ostream& out, const std::string& value) {
    return write_binary_u64(out, static_cast<uint64_t>(value.size())) &&
        (value.empty() || write_binary_exact(out, value.data(), value.size()));
}

bool read_binary_string(std::istream& in, std::string& value) {
    uint64_t size = 0;
    if (!read_binary_u64(in, size)) return false;
    value.resize(static_cast<size_t>(size));
    return size == 0 || read_binary_exact(in, value.data(), value.size());
}

bool write_binary_string_vector(std::ostream& out, const std::vector<std::string>& values) {
    if (!write_binary_u64(out, static_cast<uint64_t>(values.size()))) return false;
    for (const auto& value : values) {
        if (!write_binary_string(out, value)) return false;
    }
    return true;
}

bool read_binary_string_vector(std::istream& in, std::vector<std::string>& values) {
    uint64_t count = 0;
    if (!read_binary_u64(in, count)) return false;
    values.clear();
    values.reserve(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        std::string value;
        if (!read_binary_string(in, value)) return false;
        values.push_back(std::move(value));
    }
    return true;
}

bool write_binary_cache_header(std::ostream& out, const char magic[8], uint32_t entry_count) {
    return write_binary_exact(out, magic, 8) && write_binary_u32(out, entry_count);
}

bool read_binary_cache_header(
    std::istream& in,
    const char magic[8],
    uint32_t& entry_count,
    std::string* error_out = nullptr
) {
    char header[8];
    if (!read_binary_exact(in, header, 8)) {
        if (error_out) *error_out = "failed to read cache header";
        return false;
    }
    if (std::memcmp(header, magic, 8) != 0) {
        if (error_out) *error_out = "cache header magic mismatch";
        return false;
    }
    if (!read_binary_u32(in, entry_count)) {
        if (error_out) *error_out = "failed to read cache entry count";
        return false;
    }
    return true;
}

template <typename T, typename Writer>
bool write_debian_binary_cache(
    const std::string& path,
    const char magic[8],
    const std::vector<T>& entries,
    Writer writer,
    std::string* error_out = nullptr
) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        if (error_out) *error_out = "failed to open cache for writing";
        return false;
    }
    if (!write_binary_cache_header(out, magic, static_cast<uint32_t>(entries.size()))) {
        if (error_out) *error_out = "failed to write cache header";
        return false;
    }
    for (const auto& entry : entries) {
        if (!writer(out, entry)) {
            if (error_out) *error_out = "failed to write cache entry";
            return false;
        }
    }
    return static_cast<bool>(out);
}

template <typename T, typename Reader, typename Visitor>
bool foreach_debian_binary_cache_entry(
    const std::string& path,
    const char magic[8],
    Reader reader,
    Visitor visitor,
    std::string* error_out = nullptr
) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (error_out) *error_out = "failed to open cache";
        return false;
    }
    uint32_t entry_count = 0;
    if (!read_binary_cache_header(in, magic, entry_count, error_out)) return false;
    for (uint32_t i = 0; i < entry_count; ++i) {
        T entry;
        if (!reader(in, entry)) {
            if (error_out) *error_out = "failed to read cache entry";
            return false;
        }
        if (!visitor(entry)) return false;
    }
    return true;
}

bool write_package_metadata_binary(std::ostream& out, const PackageMetadata& meta) {
    return write_binary_string(out, meta.name) &&
        write_binary_string(out, meta.version) &&
        write_binary_string(out, meta.arch) &&
        write_binary_string(out, meta.maintainer) &&
        write_binary_string(out, meta.section) &&
        write_binary_string(out, meta.priority) &&
        write_binary_string(out, meta.filename) &&
        write_binary_string(out, meta.sha256) &&
        write_binary_string(out, meta.sha512) &&
        write_binary_string(out, meta.description) &&
        write_binary_string(out, meta.source_url);
}

bool read_package_metadata_binary(std::istream& in, PackageMetadata& meta) {
    return read_binary_string(in, meta.name) &&
        read_binary_string(in, meta.version) &&
        read_binary_string(in, meta.arch) &&
        read_binary_string(in, meta.maintainer) &&
        read_binary_string(in, meta.section) &&
        read_binary_string(in, meta.priority) &&
        read_binary_string(in, meta.filename) &&
        read_binary_string(in, meta.sha256) &&
        read_binary_string(in, meta.sha512) &&
        read_binary_string(in, meta.description) &&
        read_binary_string(in, meta.source_url);
}

bool locate_deb_data_archive(const std::string& root, std::string& out_path) {
    const char* candidates[] = {
        "/data.tar",
        "/data.tar.xz",
        "/data.tar.zst",
        "/data.tar.gz",
    };
    for (const char* suffix : candidates) {
        std::string candidate = root + suffix;
        if (access(candidate.c_str(), F_OK) == 0) {
            out_path = candidate;
            return true;
        }
    }
    return false;
}

bool materialize_deb_payload_tar(
    const std::string& archive_path,
    const std::string&,
    std::string& out_payload_tar,
    std::string* error_out = nullptr
) {
    if (access(archive_path.c_str(), F_OK) != 0) {
        if (error_out) *error_out = "payload archive is missing";
        return false;
    }
    out_payload_tar = archive_path;
    return true;
}

constexpr uint32_t DEBIAN_COMPILED_CACHE_VERSION = 1;
constexpr char DEBIAN_IMPORTED_CACHE_MAGIC[8] = {'G','P','K','G','C','A','C','1'};

std::string build_debian_catalog_shard_fingerprint(const std::string& packages_path) {
    return packages_path;
}

template <typename... Args>
bool fetch_remote_packages_index_state(Args&&...) {
    return false;
}

template <typename... Args>
bool remote_packages_index_matches_cache(Args&&...) {
    return false;
}

template <typename... Args>
bool try_update_debian_packages_with_pdiff(Args&&...) {
    return false;
}

template <typename... Args>
bool ensure_current_debian_parsed_record_cache(Args&&...) {
    return false;
}

template <typename... Args>
DebianIncrementalImportResult load_debian_index_entries_from_current_parsed_cache_incremental(Args&&...) {
    return {};
}

template <typename... Args>
std::vector<DebianPackageRecord> parse_debian_packages_file(Args&&...) {
    return {};
}

template <typename... Args>
DebianIncrementalImportResult load_debian_index_entries_from_records_incremental(Args&&...) {
    return {};
}

template <typename... Args>
std::vector<DebianSearchPreviewEntry> build_debian_search_preview_entries_from_compiled_cache(Args&&...) {
    return {};
}

template <typename... Args>
std::vector<DebianSearchPreviewEntry> build_debian_search_preview_entries_from_current_parsed_cache(Args&&...) {
    return {};
}

template <typename... Args>
bool write_debian_search_preview_cache(Args&&...) {
    return true;
}

template <typename... Args>
bool foreach_debian_search_preview_entry(Args&&...) {
    return false;
}

template <typename... Args>
bool ensure_raw_debian_context_loaded(Args&&...) {
    return false;
}

DebianBackendConfig load_debian_backend_config(bool verbose = false) {
    (void)verbose;
    return {};
}

std::string get_debian_packages_cache_path() {
    return REPO_CACHE_PATH + "native/Packages.json";
}

std::string get_debian_packages_gz_cache_path() {
    return REPO_CACHE_PATH + "native/Packages.json.gz";
}

std::string get_debian_packages_state_path() {
    return REPO_CACHE_PATH + "native/Packages.state";
}

std::string get_debian_imported_index_cache_path() {
    return REPO_CACHE_PATH + "native/imported-index.json";
}

std::string get_debian_imported_index_binary_cache_path() {
    return REPO_CACHE_PATH + "native/imported-index.bin";
}

std::string get_debian_imported_index_state_path() {
    return REPO_CACHE_PATH + "native/imported-index.state";
}

std::string get_debian_compiled_record_cache_path() {
    return REPO_CACHE_PATH + "native/compiled-records.bin";
}

std::string get_debian_compiled_record_state_path() {
    return REPO_CACHE_PATH + "native/compiled-records.state";
}

std::string build_debian_imported_index_cache_fingerprint(const std::string& packages_path) {
    if (access(packages_path.c_str(), F_OK) != 0) return "native-only:missing";
    return packages_path;
}

std::string build_debian_compiled_record_cache_policy_fingerprint() {
    return "native-only";
}

bool load_debian_packages_cache_state(const std::string&, DebianPackagesCacheState&) {
    return false;
}

bool save_debian_packages_cache_state(const std::string&, const DebianPackagesCacheState&) {
    return true;
}

bool load_debian_imported_index_cache_state(
    const std::string&,
    DebianImportedIndexCacheState&
) {
    return false;
}

bool load_debian_compiled_record_cache_state(
    const std::string&,
    DebianCompiledRecordCacheState&
) {
    return false;
}

bool debian_imported_index_cache_is_current(
    const std::string&,
    std::string* fingerprint_out = nullptr
) {
    if (fingerprint_out) fingerprint_out->clear();
    return false;
}

bool debian_compiled_record_cache_is_current() {
    return false;
}

void invalidate_debian_derived_metadata_caches(bool verbose = false) {
    (void)verbose;
}

bool ensure_current_debian_imported_index_cache(
    bool verbose,
    std::string* error_out = nullptr
) {
    (void)verbose;
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

bool load_debian_compiled_record_cache(
    const std::string&,
    const std::string&,
    std::vector<PackageMetadata>&,
    std::map<std::string, DebianCompiledRecordCacheEntry>&,
    bool
) {
    return false;
}

bool write_debian_imported_index_cache(
    const std::vector<PackageMetadata>&,
    const std::string&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

bool write_debian_compiled_record_cache(
    const std::vector<DebianCompiledRecordCacheEntry>&,
    const std::string&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

bool begin_debian_compiled_record_cache_stream(
    const std::string&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

bool append_debian_compiled_record_cache_stream_entry(
    const DebianCompiledRecordCacheEntry&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

void abort_debian_compiled_record_cache_stream(const std::string&) {}

bool finish_debian_compiled_record_cache_stream(
    const std::string&,
    const std::string&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

DebianParsedRecordLoadResult load_debian_package_records_incremental(
    const std::string&,
    const DebianBackendConfig&,
    bool
) {
    return {};
}

bool load_debian_raw_context_index(
    std::vector<RawDebianOffsetIndexEntry>&,
    std::vector<RawDebianNameIndexEntry>&,
    std::vector<RawDebianNameIndexEntry>&,
    std::string* error_out = nullptr
) {
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

bool rebuild_debian_raw_context_index(bool verbose, std::string* error_out = nullptr) {
    (void)verbose;
    if (error_out) *error_out = "Debian metadata support has been removed from gpkg";
    return false;
}

bool query_raw_debian_exact_package(
    const std::string&,
    RawDebianContext&,
    RawDebianAvailabilityResult& out_result,
    bool,
    std::string* reason_out = nullptr
) {
    out_result = {};
    if (reason_out) *reason_out = "legacy import support has been removed from gpkg";
    return false;
}

bool query_raw_debian_relation_availability(
    const std::string&,
    const std::string&,
    const std::string&,
    RawDebianContext&,
    RawDebianAvailabilityResult& out_result,
    bool,
    std::string* reason_out = nullptr
) {
    out_result = {};
    if (reason_out) *reason_out = "legacy import support has been removed from gpkg";
    return false;
}

bool resolve_raw_debian_relation_candidate(
    const std::string&,
    const std::string&,
    const std::string&,
    RawDebianContext&,
    RawDebianAvailabilityResult& out_result,
    bool,
    std::string* reason_out = nullptr
) {
    out_result = {};
    if (reason_out) *reason_out = "legacy import support has been removed from gpkg";
    return false;
}

bool get_debian_search_preview_exact_package(
    const std::string&,
    DebianSearchPreviewEntry& out_entry,
    bool,
    std::string* error_out = nullptr
) {
    out_entry = {};
    if (error_out) *error_out = "legacy import support has been removed from gpkg";
    return false;
}

std::vector<DebianSearchPreviewEntry> build_debian_search_preview_entries(
    const std::string&,
    const std::vector<PackageMetadata>&,
    const std::vector<std::string>&,
    bool
) {
    return {};
}

std::string get_cached_debian_archive_path(const PackageMetadata&) {
    return "";
}

std::string get_debian_package_url(const PackageMetadata& meta) {
    return build_repo_package_url(meta.source_url, meta.filename);
}
