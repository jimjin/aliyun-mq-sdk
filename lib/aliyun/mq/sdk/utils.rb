require "digest"
require "base64"
require "openssl"

module Aliyun::Mq::Sdk
  class Utils
  end

  class Auth
    class << self
      def post_sign(secret_key, topic, producer_id, msg, date)
        build_sign([secret_key, topic, producer_id, md5(msg), date].join("\n"), secret_key)
      end

      def get_sign(secret_key, topic, consumer_id, date)
        build_sign([secret_key, topic, consumer_id, date].join("\n"), secret_key)
      end

      def del_sign(secret_key, topic, consumer_id, msg_handle, date)
        build_sign([secret_key, topic, consumer_id, msg_handle, date].join("\n"), secret_key)
      end

      def build_sign(sign_str, secret_key)
        Base64.encode64("#{OpenSSL::HMAC.digest("sha1", secret_key, sign_str)}").strip
      end
    end
  end
end