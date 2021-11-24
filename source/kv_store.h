#include <unordered_map>
#include <memory>




class KV_store{
	public:
		static std::shared_ptr<KV_store> init() {
			return std::make_shared<KV_store>();
		}

		bool put(int key, std::string value) {
			if (kv_store.find(key) != kv_store.end())
				kv_store[key] = value;
			else
				kv_store.insert({key, value});
			return true;
		}
		
		bool get(int key, std::string& value) {
			if (kv_store.find(key) != kv_store.end()) {
				value = kv_store[key];
				return true;
			}
			else{
				value = "\0";
				return false;
			}
		}

	private:
		std::unordered_map<int, std::string> kv_store;
};
