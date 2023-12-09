package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

import java.sql.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
//        throw new UnsupportedOperationException("TODO: replace this with your own student id");
        return Arrays.asList(12212108);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        String sql_Danmu = "INSERT INTO \"DanmuRecord\"(Bv, Mid, Time, Content, PostTime, LikedBy, Danmu_is_Deleted) VALUES (?, ?, ?, ?, ?, ?, ?)";
        String sql_User = "INSERT INTO \"UserRecord\"(Mid, Name, Sex, Birthday, Level, Coin, Sign, Following, Identity, Password, QQ, WeChat, User_is_Deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String sql_Video = "INSERT INTO \"VideoRecord\"(Bv, Title, OwnerMid, OwnerName, CommitTime, ReviewTime, PublicTime, Duration, Description, Reviewer, \"Like\", Coin, Favorite, ViewerMids, ViewTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connDanmu = dataSource.getConnection();
             Connection connUser = dataSource.getConnection();
             Connection connVideo = dataSource.getConnection();
             PreparedStatement stmtDanmu = connDanmu.prepareStatement(sql_Danmu);
             PreparedStatement stmtUser = connUser.prepareStatement(sql_User);
             PreparedStatement stmtVideo = connVideo.prepareStatement(sql_Video)
        ) {
            // Insert Danmu Records
            for (DanmuRecord danmuRecord : danmuRecords) {
                stmtDanmu.setString(1, danmuRecord.getBv());
                stmtDanmu.setLong(2, danmuRecord.getMid());
                stmtDanmu.setFloat(3, danmuRecord.getTime());
                stmtDanmu.setString(4, danmuRecord.getContent());
                stmtDanmu.setTimestamp(5, danmuRecord.getPostTime());
                Long[] likedByArray = Arrays.stream(danmuRecord.getLikedBy()).boxed().toArray(Long[]::new);
                Array likedBySqlArray = connDanmu.createArrayOf("BIGINT", likedByArray);
                stmtDanmu.setArray(6, likedBySqlArray);
                stmtDanmu.setBoolean(7, danmuRecord.isDanmu_is_Deleted());
                log.info("SQL: {}", stmtDanmu);
                stmtDanmu.executeUpdate();
                likedBySqlArray.free();
            }

            // Insert User Records
            for (UserRecord userRecord : userRecords) {
                stmtUser.setLong(1, userRecord.getMid());
                stmtUser.setString(2, userRecord.getName());
                stmtUser.setString(3, userRecord.getSex());
                stmtUser.setString(4, userRecord.getBirthday());
                stmtUser.setShort(5, userRecord.getLevel());
                stmtUser.setInt(6, userRecord.getCoin());
                stmtUser.setString(7, userRecord.getSign());
                stmtUser.setString(8, userRecord.getIdentity().name());
                stmtUser.setString(9, userRecord.getPassword());
                stmtUser.setString(10, userRecord.getQq());
                stmtUser.setString(11, userRecord.getWechat());
                Long[] followingArray = Arrays.stream(userRecord.getFollowing()).boxed().toArray(Long[]::new);
                Array followingSqlArray = connUser.createArrayOf("BIGINT", followingArray);
                stmtUser.setArray(12, followingSqlArray);
                stmtUser.setBoolean(13, userRecord.isUser_is_Deleted());
                log.info("SQL: {}", stmtUser);
                stmtUser.executeUpdate();
                followingSqlArray.free();
            }

            // Insert Video Records
            for (VideoRecord videoRecord : videoRecords) {
                stmtVideo.setString(1, videoRecord.getBv());
                stmtVideo.setString(2, videoRecord.getTitle());
                stmtVideo.setLong(3, videoRecord.getOwnerMid());
                stmtVideo.setString(4, videoRecord.getOwnerName());
                stmtVideo.setTimestamp(5, videoRecord.getCommitTime());
                stmtVideo.setTimestamp(6, videoRecord.getReviewTime());
                stmtVideo.setTimestamp(7, videoRecord.getPublicTime());
                stmtVideo.setFloat(8, videoRecord.getDuration());
                stmtVideo.setString(9, videoRecord.getDescription());
                stmtVideo.setLong(10, videoRecord.getReviewer());
                Long[] likeArray = Arrays.stream(videoRecord.getLike()).boxed().toArray(Long[]::new);
                Array likeSqlArray = connVideo.createArrayOf("BIGINT", likeArray);
                stmtVideo.setArray(11, likeSqlArray);
                Long[] coinArray = Arrays.stream(videoRecord.getCoin()).boxed().toArray(Long[]::new);
                Array coinSqlArray = connVideo.createArrayOf("BIGINT", coinArray);
                stmtVideo.setArray(12, coinSqlArray);
                Long[] favoriteArray = Arrays.stream(videoRecord.getFavorite()).boxed().toArray(Long[]::new);
                Array favoriteSqlArray = connVideo.createArrayOf("BIGINT", favoriteArray);
                stmtVideo.setArray(13, favoriteSqlArray);
                Long[] viewerMidsArray = Arrays.stream(videoRecord.getViewerMids()).boxed().toArray(Long[]::new);
                Array viewerMidsSqlArray = connVideo.createArrayOf("BIGINT", viewerMidsArray);
                stmtVideo.setArray(14, viewerMidsSqlArray);
                // Convert float array to Double array
                Double[] viewTimeArray = new Double[videoRecord.getViewTime().length];
                for (int i = 0; i < viewTimeArray.length; i++) {
                    viewTimeArray[i] = (double) videoRecord.getViewTime()[i];
                }
// Use the Double array in the SQL statement
                stmtVideo.setArray(15, connVideo.createArrayOf("FLOAT8", viewTimeArray));
                log.info("SQL: {}", stmtVideo);
                stmtVideo.executeUpdate();
                likeSqlArray.free();
                coinSqlArray.free();
                favoriteSqlArray.free();
                viewerMidsSqlArray.free();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
