import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;


// TODO:
//1. Every server node has a secret key, which they use for symmetric encryption/decryption of authorization tokens.
//		-the key is created upon node construction, and is stored persistently
//		-reloaded upon restart
//		-filename is "secretKey.txt"
//		-all servers share the same secret key, to facilitate sharing of authentication tokens between servers
//2. Servers will be preloaded with files called "passkeyStore.txt", which will all be identical
//		-the files have one record per user: name,hash,salt
//3. Upon login, client the password to some server, transmits that with the login
//4. Server hashes the password, along with the random salt, and checks that against the passkey store
//5. Server generates an authentication token by encrypting <address of logging in node><username><salt>


public class EncryptTest {
	
	
	
	public static void main(String[] args) {
		String s = "blahblahblah";
		byte[] clearText = s.getBytes();
		
		
		try {
			KeyGenerator kg = KeyGenerator.getInstance("AES");
			Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
			
			//server's secret key
			SecretKey k = kg.generateKey();
			
			//a second random string used for a salt byte source
			SecretKey saltBytes = kg.generateKey();
			
			
			//salt the cleartext
			byte[] saltedCT = new byte[clearText.length + 4];
			System.arraycopy(saltBytes.getEncoded(), 0, saltedCT, 0, 4);
			System.arraycopy(clearText, 0, saltedCT, 4, clearText.length);
			
			//encrypt the cleartext			
			c.init(Cipher.ENCRYPT_MODE, k);
			byte[] cipherText = c.doFinal(saltedCT);
			
			//now lets append the MAC (this is encrypt-then-mac paradigm)
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] mac = md.digest(cipherText);
						
			byte[] finalMsg = new byte[cipherText.length + mac.length];
			System.arraycopy(cipherText, 0, finalMsg, 0, cipherText.length);
			System.arraycopy(mac, 0, finalMsg, cipherText.length, mac.length);
			
			//--------------------Received------------------------------
			
			//Check the mac:
			byte[] act = new byte[finalMsg.length-32];
			System.arraycopy(finalMsg,0,act,0,finalMsg.length-32);
			byte[] am = new byte[32];
			System.arraycopy(finalMsg,finalMsg.length-32,am,0,32);	
			
			byte[] digest = md.digest(act);
					
			if(MessageDigest.isEqual(am, digest)){
				System.out.println("verified");
			}
			else{
				System.out.println("not so verified: ");
				System.out.println(new String(am));
				System.out.println(new String(digest));
			}
			
			
			//now lets decrypt it to see how it looks  (will include the salt at the beginning)
			c.init(Cipher.DECRYPT_MODE, k);
			System.out.println("Decrypted: " + new String(c.doFinal(cipherText)));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
	}

}
