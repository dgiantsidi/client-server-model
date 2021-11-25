#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

class KvStore {
public:
  static inline auto init() -> std::shared_ptr<KvStore> {
    return std::make_shared<KvStore>();
  }

  inline auto put(int key, std::string_view value) -> bool {
    kv_store.insert_or_assign(key, value);
    return true;
  }

  inline auto get(int key) const -> std::optional<std::string_view> {
    auto it = kv_store.find(key);
    if (it == kv_store.end()) {
      return std::nullopt;
    }
    return it->second;
  }

private:
  std::unordered_map<int, std::string> kv_store;
};
