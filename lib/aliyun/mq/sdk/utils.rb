require "digest"
require "base64"
require "openssl"
require 'digest/sha1'

module Aliyun::Mq::Sdk
  class Utils
    class << self
      def symbolize_keys(h)
        h = h.inject({}){|memo, (k, v)| memo[k.to_sym] = v; memo}
      end

      def deep_symbolize_keys(obj)
        if obj.is_a?(Array)
          obj.map do |item|
            deep_symbolize_keys(item)
          end
        elsif obj.is_a?(Hash)
          obj.inject({}) do |memo, (k, v)|
            memo[k.to_sym] = deep_symbolize_keys(v)
            memo
          end
        else
          obj
        end
      end
    end
  end

  class Auth
    class << self
      def post_sign(secret_key, topic, producer_id, msg, date)
        s = build_sign([topic, producer_id, md5(msg), date].join("\n"), secret_key)
      end

      def get_sign(secret_key, topic, consumer_id, date)
        build_sign([topic, consumer_id, date].join("\n"), secret_key)
      end

      def del_sign(secret_key, topic, consumer_id, msg_handle, date)
        build_sign([topic, consumer_id, msg_handle, date].join("\n"), secret_key)
      end

      def build_sign(sign_str, secret_key)
        Base64.strict_encode64("#{OpenSSL::HMAC.digest('sha1', secret_key, sign_str)}").strip
      end

      def md5(cnt)
        Digest::MD5.hexdigest(cnt)
      end
    end
  end
end