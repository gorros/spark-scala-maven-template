package com.github.gorros.helpers

import java.nio.ByteBuffer
import java.util.Base64

import com.amazonaws.services.kms.AWSKMSClientBuilder
import com.amazonaws.services.kms.model.{DecryptRequest, EncryptRequest}


object KMSUtils {
    private lazy val kmsClient = AWSKMSClientBuilder.defaultClient

    // You can use these methods to encrypt and decrypt up to 4 KB (4096 bytes) of data.
    def encryptString(plainText: String, keyArn: String): String = {
        val req = new EncryptRequest().withKeyId(keyArn).withPlaintext(ByteBuffer.wrap(plainText.getBytes))
        Base64.getEncoder.encodeToString(kmsClient.encrypt(req).getCiphertextBlob.array())
    }

    def decryptString(encryptedText: String, keyArn: String): String = {
        val req = new DecryptRequest().withCiphertextBlob(ByteBuffer.wrap(Base64.getDecoder.decode(encryptedText)))
        new String(kmsClient.decrypt(req).getPlaintext.array())
    }

    def main(args: Array[String]): Unit = {
        println("Enter key arn: ")
        val keyArn = scala.io.StdIn.readLine()
        val encrypted = encryptString("test", keyArn)
        println(decryptString(encrypted, keyArn))
    }
}
