package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
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
    public void importData(//TODO:需要实现对某些插入条件的限制
                           List<DanmuRecord> danmuRecords,
                           List<UserRecord> userRecords,
                           List<VideoRecord> videoRecords
    ) {

        String sql_Danmu = "INSERT INTO \"DanmuRecord\"(Bv, Mid, Time, Content, PostTime, LikedBy, Danmu_is_Deleted,id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String sql_User = "INSERT INTO \"UserRecord\"(Mid, Name, Sex, Birthday, Level, Coin, Sign, Identity, Password, QQ, WeChat,Following, User_is_Deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ? , ?, ?, ?, ?, ?)";
        String sql_Video = "INSERT INTO \"VideoRecord\"(Bv, Title, OwnerMid, OwnerName, CommitTime, ReviewTime, PublicTime, Duration, Description, Reviewer, \"Like\", Coin, Favorite, ViewerMids, ViewTime, Video_is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
        String sql_UserFollowing = "INSERT INTO user_following(user_1, user_2) VALUES (?, ?)";
        String sql_ViewerMids = "Insert into view_video (Bv, View_Mid, View_Time) VALUES (?,?,?)";
        String sql_ViewLike = "Insert into video_like (Bv, Video_LIKE_Mid) values (?,?)";
        String sql_CollectVideo = "insert into video_collect (bv, Collected_Mid) values (?,?)";
        String sql_video_coin = "Insert into coin_user (bv, coin_mid) VALUES (?,?)";
        String sql_danmu_like = "INSERT INTO danmu_like (bv, danmu_like_mid, id) VALUES (?,?,?)";
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());


        try (Connection connDanmu = dataSource.getConnection();
             Connection connUser = dataSource.getConnection();
             Connection connVideo = dataSource.getConnection();
             PreparedStatement stmtDanmu = connDanmu.prepareStatement(sql_Danmu);
             PreparedStatement stmtUser = connUser.prepareStatement(sql_User);
             PreparedStatement stmtVideo = connVideo.prepareStatement(sql_Video);
             PreparedStatement stmtUserFollowing = connUser.prepareStatement(sql_UserFollowing);
             PreparedStatement stmtViewerMids = connVideo.prepareStatement(sql_ViewerMids);
             PreparedStatement stmtVideoLike = connVideo.prepareStatement(sql_ViewLike);
             PreparedStatement stmtCollectVideo = connVideo.prepareStatement(sql_CollectVideo);
             PreparedStatement stmt_video_coin = connVideo.prepareStatement(sql_video_coin);
             PreparedStatement stmt_danmu_like = connDanmu.prepareStatement(sql_danmu_like);
        ) {
            // Insert Danmu Records
            for (DanmuRecord danmuRecord : danmuRecords) {
                long id = generate_danmu_id(danmuRecord.getMid());
                stmtDanmu.setString(1, danmuRecord.getBv());
                stmtDanmu.setLong(2, danmuRecord.getMid());
                stmtDanmu.setFloat(3, danmuRecord.getTime());
                stmtDanmu.setString(4, danmuRecord.getContent());
                stmtDanmu.setTimestamp(5, danmuRecord.getPostTime());
                Long[] likedByArray = Arrays.stream(danmuRecord.getLikedBy()).boxed().toArray(Long[]::new);
                //将数据插入到danmu_like表格中
                for (Long likedBy : likedByArray) {
                    stmt_danmu_like.setString(1, danmuRecord.getBv());
                    stmt_danmu_like.setLong(2, likedBy);
                    stmt_danmu_like.setLong(3, id);
                    log.info("SQL: {}", stmt_danmu_like);
                    stmt_danmu_like.executeUpdate();
                }
                Array likedBySqlArray = connDanmu.createArrayOf("BIGINT", likedByArray);
                stmtDanmu.setArray(6, likedBySqlArray);
                stmtDanmu.setBoolean(7, danmuRecord.isDanmu_is_Deleted());
                stmtDanmu.setLong(8, generate_danmu_id(id));
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
                // 遍历关注列表，插入到user_following表中
                for (Long following : followingArray) {
                    stmtUserFollowing.setLong(1, userRecord.getMid());
                    stmtUserFollowing.setLong(2, following);
                    stmtUserFollowing.executeUpdate();
                }
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
                //插入点赞表格，方便后续查询。
                for (Long videolike : likeArray) {
                    stmtVideoLike.setString(1, videoRecord.getBv());
                    stmtVideoLike.setLong(2, videolike);
                    stmtVideoLike.executeUpdate();

                }
                Array likeSqlArray = connVideo.createArrayOf("BIGINT", likeArray);
                stmtVideo.setArray(11, likeSqlArray);
                Long[] coinArray = Arrays.stream(videoRecord.getCoin()).boxed().toArray(Long[]::new);
                //插入投币表格
                for (Long video_coin : coinArray) {
                    stmt_video_coin.setString(1, videoRecord.getBv());
                    stmt_video_coin.setLong(2, video_coin);
                    stmt_video_coin.executeUpdate();
                }
                Array coinSqlArray = connVideo.createArrayOf("BIGINT", coinArray);
                stmtVideo.setArray(12, coinSqlArray);
                Long[] favoriteArray = Arrays.stream(videoRecord.getFavorite()).boxed().toArray(Long[]::new);
                for (Long favorite : favoriteArray) {
                    stmtCollectVideo.setString(1, videoRecord.getBv());
                    stmtCollectVideo.setLong(2, favorite);
                    log.info("SQL: {}", stmtCollectVideo);
                    stmtCollectVideo.executeUpdate();
                }
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
                for (int i = 0; i < viewerMidsArray.length; i++) {
                    stmtViewerMids.setString(1, videoRecord.getBv());
                    stmtViewerMids.setLong(2, viewerMidsArray[i]);
                    stmtViewerMids.setDouble(3, viewTimeArray[i]);
                    stmtViewerMids.executeUpdate();
                }
// Use the Double array in the SQL statement
                stmtVideo.setArray(15, connVideo.createArrayOf("FLOAT8", viewTimeArray));
                stmtVideo.setBoolean(16, false);
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

    public long generate_danmu_id(long mid) {
        long timestamp = System.currentTimeMillis();
        return (mid << 32) | (timestamp & 0xFFFFFFFFL);
    }
}
