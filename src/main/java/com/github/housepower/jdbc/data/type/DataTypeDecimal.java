package com.github.housepower.jdbc.data.type;

import com.github.housepower.jdbc.connect.PhysicalInfo;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.data.type.complex.DataTypeDateTime;
import com.github.housepower.jdbc.misc.SQLLexer;
import com.github.housepower.jdbc.misc.StringView;
import com.github.housepower.jdbc.misc.Validate;
import com.github.housepower.jdbc.serializer.BinaryDeserializer;
import com.github.housepower.jdbc.serializer.BinarySerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

public class DataTypeDecimal implements IDataType {

    /**
     * Decimal32 9
     * Decimal64 18
     * Decimal128 38
     * 精度 Decimal(18,2)  Decimal(9,2)
     */
    private int precision;

    /**
     * 模
     */
    private int scale;

    private int size;

    public DataTypeDecimal(int precision,int scale){
        this.precision = precision;
        this.scale = scale;
        parseSize();
    }

    @Override
    public String name() {
        return String.format("Decimal(%d,%d)",precision,scale);
    }

    @Override
    public int sqlTypeId() {
        return Types.DECIMAL;
    }

    @Override
    public Object defaultValue() {
        return BigDecimal.ZERO;
    }

    @Override
    public Class javaTypeClass() {
        return BigDecimal.class;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof StringView) {
            String ss = data.toString();
            BigDecimal bigDecimal = new BigDecimal(ss);
            bigDecimal.movePointRight(scale);
            BigInteger bigInteger = bigDecimal.unscaledValue();
            byte[] bs2 = new byte[size];
            Arrays.fill(bs2,(byte)0);
            byte[] bs = bigInteger.toByteArray();
            if(bs.length > size){
                throw new SQLException(String.format("this value %s is too big",ss));
            }
            int length = bs.length;
            for(int i = 0;i<length;i++){
                bs2[i]=bs[length-i-1];
            }
            serializer.writeBytes(bs2);
        } else {
            throw new SQLException("Expected String Parameter, but was " + data.getClass().getSimpleName());
        }
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bs = new byte[size];
        bs = deserializer.readBytes(size);
        byte[] bs2 = new byte[size];
        for(int i = 0;i<size;i++){
            bs2[i]=bs[size-i-1];
        }
        return new BigDecimal(new BigInteger(bs2),scale);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer)
        throws SQLException, IOException {
        BigDecimal[] data = new BigDecimal[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = (BigDecimal)deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(SQLLexer lexer) throws SQLException {
        return lexer.stringLiteral();
    }

    public static IDataType createDecimalType(SQLLexer lexer, PhysicalInfo.ServerInfo serverInfo) throws SQLException {
        if (lexer.isCharacter('(')) {
            Validate.isTrue(lexer.character() == '(');
            int precision = lexer.numberLiteral().intValue();
            Validate.isTrue(lexer.character() == ',');
            int scale = lexer.numberLiteral().intValue();
            Validate.isTrue(lexer.character() == ')');
            return new DataTypeDecimal(precision,scale);
        }
        throw new SQLException("Unrecognized the "+lexer.toString());
    }

    private int parseSize(){
        if(precision<=9){
            this.size = 4;
        } else if(precision<=18){
            this.size = 8;
        } else if(precision<=38){
            this.size = 16;
        }
        return 0;
    }
}
