require "digest"
require "base64"
require "openssl"
require 'digest/sha1'
Digest::SHA1.hexdigest 'foo'

module Aliyun::Mq::Sdk
  class Utils
  end

  class Auth
    class << self
      def post_sign(secret_key, topic, producer_id, msg, date)
        p [secret_key, topic, producer_id, md5(msg), date].join("\n")
        s = build_sign([topic, producer_id, md5(msg), date].join("\n"), secret_key)
        p s
        s
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