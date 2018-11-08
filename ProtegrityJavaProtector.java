package com.dfsi.dsol.fastdata.common.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.protegrity.ap.java.ProtectorException;
import com.protegrity.ap.java.SessionObject;
import com.protegrity.ap.java.SessionTimeoutException;

public class ProtegrityJavaProtector implements Protector {

	Logger logger = LoggerFactory.getLogger(getClass());

	String policyMember = System.getProperty("user.name");
	String extInitVector = "";
	String lastError = "";
	Boolean apiResult = false;

	byte[][] unprotectedData = new byte[1][];
	byte[][] protectByteArray = new byte[1][];
	String[] inputStringArray = new String[1];
	String dataElementName = "DE_PAN23";
	SessionObject session = null;
	com.protegrity.ap.java.Protector api = null;

	public ProtegrityJavaProtector() throws Exception {
		try {
			init();
		} catch (ProtectorException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void init() throws Exception {
		try {

			api = com.protegrity.ap.java.Protector.getProtector();
			System.out.println("Policy Member " + policyMember);
			session = api.createSession(policyMember);
		} catch (ProtectorException e) {
			System.out.println("Error creating Session: ");
			e.printStackTrace();
		}
	}

	public void createSession() {
		try {
			api.createSession(policyMember);
		} catch (ProtectorException e) {
			e.printStackTrace();
		}
	}

	public String protect(final String number) {
		String protectedASCIIData = null;
		try {

			inputStringArray[0] = number;
			apiResult = api.protect(session, dataElementName, inputStringArray, protectByteArray,
					extInitVector.getBytes());

			if (protectByteArray[0] == null) {
				protectByteArray[0] = "".getBytes();
			}

			protectedASCIIData = new String(protectByteArray[0]);
			System.out.println("  Protected (ASCII): " + protectedASCIIData);

			lastError = api.getLastError(session);
			if (!apiResult) {
				logger.error("\r  Error Description: " + lastError);
			}
		} catch (SessionTimeoutException e) {
			e.printStackTrace();
		} catch (ProtectorException e) {
			e.printStackTrace();
		}
		return protectedASCIIData;
	}

	public String unProtect(String input) {
		String unprotectedASCIIData = "";
		try {
			protectByteArray[0] = input.getBytes();

			apiResult = api.unprotect(session, dataElementName, protectByteArray, unprotectedData,
					extInitVector.getBytes());

			if (unprotectedData[0] == null) {
				unprotectedData[0] = "".getBytes();
			}
			unprotectedASCIIData = new String(unprotectedData[0]);

			lastError = api.getLastError(session);
			if (!apiResult) {
				logger.error("\r  Error Description: " + lastError);
			}

		} catch (ProtectorException e) {
			e.printStackTrace();
		}

		return unprotectedASCIIData;
	}

	public void close() {
		try {
			api.closeSession(session);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
