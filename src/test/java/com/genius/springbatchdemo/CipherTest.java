package com.genius.springbatchdemo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CipherTest {
	@DisplayName("Testing filterByCity")
	@Test
	void cipherTest() {
		String plainText="ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		String cipherText="DEFGHIJKLMNOPQRSTUVWXYZABC";
		assertEquals(cipherText, CeaserCipher.cipher(plainText));
	}
}
